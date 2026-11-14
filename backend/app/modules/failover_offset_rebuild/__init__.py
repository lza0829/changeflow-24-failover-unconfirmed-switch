"""failover_offset_rebuild —— 故障后位点重建。

当流上的 ``apply_cursor`` 丢失或被改坏时，**不能清零瞎跑**：清零会把已经
落过 Sink 的事件再应用一遍。本模块只做一件事——把 Sink、已应用账本
（``applied_seqs`` / ``change_ledger``，隔离区 ``quarantine`` 作为旁证）当作
证据，算出一个可安全恢复的候选位点，并经人工确认后才允许切换游标。

能力边界（与相邻切口划清）：

- **不是回拨重放**（cursor_rewind_window / idempotent_replay）：本模块不重放
  任何事件、不改 Sink、不改账本，只重建流级游标这一个数字。
- **不是行级校验/漂移门禁**（sink_checksum_reconcile）：不逐行比对 Sink 与
  账本的 payload 内容，只核对位点层面的 seq 归属与连续前缀。

公开算法见 :func:`compute_candidate`。

算法（对单个 stream 的证据集合）：

  A = applied_seqs 中该流全部已应用 seq（已应用账本，位点事实的主证据）
  S = { sink_records.last_seq }（Sink 侧证据；删除的实体不留痕，故 S 只可能偏小）
  Q = quarantine 中该流全部 seq（旁证：这些 seq 被裁决为未应用）
  L = change_ledger 中 disposition='applied' 的 seq（账本第二来源，交叉核对）

候选位点 candidate = 从 1 起 A 的最长连续前缀长度。从该位点恢复意味着
seq 1..candidate 都已有应用事实、candidate+1 尚未应用，因此绝不会重复应用。

切换必须同时通过以下门禁，任一不过即拒绝切换（不允许标记重建成功）：

  G1 证据充分：A 与 S 不能同时为空（没有任何应用事实，无从定位）。
  G2 Sink 不得超出账本：max(S) 必须落在 A 内；否则存在“Sink 有、账本无”的
     seq，两边证据打架。
  G3 Sink 不得越过连续前缀：max(S) <= candidate；否则账本在
     max(S) 之前存在未解释缺口，candidate 无法代表 Sink 已到达的位点。
  G4 隔离区与已应用集合互斥：Q ∩ A 必须为空；同一 seq 既被隔离又被标记
     已应用，属自相矛盾的证据。
  G5 账本两来源一致：L 必须与 A 完全相等；否则 change_ledger 与
     applied_seqs 互相打架。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass
class Evidence:
    """单流证据快照（由 repository 层扫库组装）。"""

    stream_id: int
    applied_seqs: list[int]
    sink_last_seqs: list[int]
    quarantine_seqs: list[int]
    ledger_applied_seqs: list[int]
    scan_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class CandidateResult:
    candidate: int
    adoptable: bool
    gate: str  # "ok" 或被拦下的门禁编号 G1..G5
    reasons: list[str]
    basis: list[str]  # 可展示的计算依据（人话）
    detail: dict[str, Any]


def _longest_contiguous_prefix(seqs: Iterable[int]) -> int:
    ordered = sorted(set(seqs))
    cursor = 0
    for seq in ordered:
        if seq == cursor + 1:
            cursor = seq
        elif seq <= cursor:
            continue
        else:
            break
    return cursor


def evidence_fingerprint(evidence: Evidence) -> str:
    """证据快照指纹：确认时据此判断任务挂起期间证据是否变化。"""
    payload = {
        "stream_id": evidence.stream_id,
        "applied_seqs": sorted(set(evidence.applied_seqs)),
        "sink_last_seqs": sorted(set(evidence.sink_last_seqs)),
        "quarantine_seqs": sorted(set(evidence.quarantine_seqs)),
        "ledger_applied_seqs": sorted(set(evidence.ledger_applied_seqs)),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def evidence_from_detail(detail: dict[str, Any]) -> Evidence:
    """从任务里持久化的证据摘要还原证据快照（对照接口与任务同源）。

    旧版本任务未持久化 ``ledger_applied_seqs``；缺失时按 ``applied_seqs`` 兜底，
    避免把历史快照误判成“账本两来源冲突”。当前证据仍以实时扫库为准。
    """
    applied = [int(x) for x in detail.get("applied_seqs") or []]
    ledger = detail.get("ledger_applied_seqs")
    return Evidence(
        stream_id=int(detail.get("stream_id") or 0),
        applied_seqs=applied,
        sink_last_seqs=[int(x) for x in detail.get("sink_last_seqs") or []],
        quarantine_seqs=[int(x) for x in detail.get("quarantine_seqs") or []],
        ledger_applied_seqs=(
            [int(x) for x in ledger] if ledger is not None else list(applied)
        ),
    )


def side_scores(evidence: Evidence) -> dict[str, Any]:
    """账本侧 / Sink 侧各自的候选位点打分与冲突判定（只读、纯函数）。

    任务视图、证据对照接口、页面三处都调用本函数，保证同源。
    """
    applied = set(evidence.applied_seqs)
    ledger_applied = set(evidence.ledger_applied_seqs)
    sink_seq_set = set(evidence.sink_last_seqs)

    ledger_prefix = _longest_contiguous_prefix(applied)
    ledger_source_prefix = _longest_contiguous_prefix(ledger_applied)
    sink_max = max(evidence.sink_last_seqs, default=0)

    only_in_applied = sorted(applied - ledger_applied)
    only_in_ledger = sorted(ledger_applied - applied)
    unexplained_sink = sorted(sink_seq_set - applied)
    sink_ahead = max(0, sink_max - ledger_prefix)
    quarantine_overlap = sorted(set(evidence.quarantine_seqs) & applied)

    ledger_side = {
        "candidate_score": ledger_prefix,
        "ledger_source_candidate_score": ledger_source_prefix,
        "applied_count": len(applied),
        "applied_max_seq": max(applied, default=0),
        "ledger_applied_count": len(ledger_applied),
        "sources_agree": applied == ledger_applied,
        "only_in_applied": only_in_applied,
        "only_in_ledger": only_in_ledger,
        "quarantine_overlap": quarantine_overlap,
    }
    sink_side = {
        "candidate_score": sink_max,
        "record_count": len(sink_seq_set),
        "sink_last_seqs": sorted(sink_seq_set),
        "unexplained_seqs": unexplained_sink,
        "ahead_of_ledger_by": sink_ahead,
    }

    conflict_reasons: list[str] = []
    if only_in_applied or only_in_ledger:
        conflict_reasons.append(
            "账本两来源不一致：applied_seqs 独有 "
            f"{only_in_applied}，change_ledger(applied) 独有 {only_in_ledger}"
        )
    if unexplained_sink:
        conflict_reasons.append(
            "Sink 出现账本中不存在的 last_seq：" + ",".join(map(str, unexplained_sink))
        )
    if sink_ahead > 0:
        conflict_reasons.append(
            f"Sink 候选分 {sink_max} 越过账本连续前缀 {ledger_prefix}（缺口 {sink_ahead}）"
        )
    if quarantine_overlap:
        conflict_reasons.append(
            "同一 seq 既在隔离区又被标记已应用：" + ",".join(map(str, quarantine_overlap))
        )

    return {
        "stream_id": evidence.stream_id,
        "evidence_fingerprint": evidence_fingerprint(evidence),
        "sufficient": bool(applied or sink_seq_set),
        "ledger_side": ledger_side,
        "sink_side": sink_side,
        "conflict": bool(conflict_reasons),
        "conflict_reasons": conflict_reasons,
        "agree": not conflict_reasons,
    }


def compute_candidate(evidence: Evidence) -> CandidateResult:
    """纯函数：根据证据快照计算候选位点并执行全部切换门禁。"""
    applied = set(evidence.applied_seqs)
    sink_seq_set = set(evidence.sink_last_seqs)
    quarantined = set(evidence.quarantine_seqs)
    ledger_applied = set(evidence.ledger_applied_seqs)

    candidate = _longest_contiguous_prefix(applied)
    max_sink = max(evidence.sink_last_seqs, default=0)
    detail: dict[str, Any] = {
        "stream_id": evidence.stream_id,
        "evidence_fingerprint": evidence_fingerprint(evidence),
        "applied_max": max(applied, default=0),
        "applied_count": len(applied),
        "sink_max_last_seq": max_sink,
        "sink_record_count": len(sink_seq_set),
        "quarantine_count": len(quarantined),
        "ledger_applied_count": len(ledger_applied),
        "applied_seqs": sorted(applied),
        "sink_last_seqs": sorted(sink_seq_set),
        "quarantine_seqs": sorted(quarantined),
        "ledger_applied_seqs": sorted(ledger_applied),
    }
    basis = [
        f"applied_seqs 共 {len(applied)} 条，max={max(applied, default=0)}",
        f"sink_records 共 {len(sink_seq_set)} 行，max(last_seq)={max_sink}",
        f"quarantine 共 {len(quarantined)} 条旁证",
        f"change_ledger(applied) 共 {len(ledger_applied)} 条",
        f"候选位点 = applied_seqs 从 1 起最长连续前缀 = {candidate}",
    ]
    reasons: list[str] = []

    # G1 证据充分
    if not applied and not sink_seq_set:
        reasons.append("G1 证据不足：applied_seqs 与 sink_records 均为空，没有任何应用事实可定位位点")
        return CandidateResult(candidate, False, "G1", reasons, basis, detail)

    # G2 Sink 不得超出账本
    sink_outside = sorted(sink_seq_set - applied)
    if sink_outside:
        reasons.append(
            "G2 证据冲突：以下 Sink last_seq 在 applied_seqs 中找不到应用记录："
            + ",".join(map(str, sink_outside))
        )

    # G3 Sink 不得越过连续前缀
    if max_sink > candidate:
        missing = sorted(s for s in range(1, max_sink + 1) if s not in applied)
        shown = missing[:10]
        more = f" 等 {len(missing)} 个" if len(missing) > 10 else ""
        reasons.append(
            f"G3 证据冲突：Sink 已到达 seq {max_sink}，但连续前缀只到 {candidate}，"
            f"账本在其之前存在未解释缺口 {shown}{more}"
        )

    # G4 隔离区与已应用集合互斥
    overlap = sorted(quarantined & applied)
    if overlap:
        reasons.append(
            "G4 证据冲突：以下 seq 同时存在于隔离区与 applied_seqs：" + ",".join(map(str, overlap))
        )

    # G5 账本两来源一致
    only_in_applied = sorted(applied - ledger_applied)
    only_in_ledger = sorted(ledger_applied - applied)
    if only_in_applied or only_in_ledger:
        reasons.append(
            "G5 账本自相矛盾：applied_seqs 独有 "
            f"{only_in_applied}，change_ledger(applied) 独有 {only_in_ledger}"
        )

    if reasons:
        return CandidateResult(candidate, False, "rejected", reasons, basis, detail)

    basis.append("全部门禁 G1-G5 通过：证据一致且充分，候选位点可采纳")
    return CandidateResult(candidate, True, "ok", [], basis, detail)
