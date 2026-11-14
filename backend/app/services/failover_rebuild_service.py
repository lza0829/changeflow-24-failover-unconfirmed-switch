"""failover_offset_rebuild 服务层：扫证据 → 候选 → 人工确认才切换。

三条不可破坏的约束：

1. **未确认前禁止改写流游标**：``start_rebuild`` 只读证据、只写任务记录。
2. **证据不足/矛盾必须拒绝切换**：门禁不过的任务落 ``rejected``，游标保持任务前值。
3. **审计双相**：创建任务写 ``proposal``（cursor_before / candidate /
   evidence_fingerprint / gate），confirm/reject 追加 ``decision``
   （action / cursor_after / untouched）。
"""

from __future__ import annotations

from typing import Any

from app.db import connect
from app.modules.failover_offset_rebuild import (
    Evidence,
    compute_candidate,
    evidence_from_detail,
    evidence_fingerprint,
    side_scores,
)
from app.repositories import failover_rebuild as rebuild_repo
from app.repositories import sink as sink_repo
from app.repositories import streams as streams_repo

DEMO_ENTITY_PREFIX = "__rebuild_demo__"

# 只有这个状态允许人工决策；其余一律视为终态。
PENDING = "awaiting_confirmation"


def _live_cursor(conn, stream_id: int) -> int:
    stream = streams_repo.get_stream(conn, stream_id)
    if not stream:
        raise ValueError("stream not found")
    return int(stream["apply_cursor"] or 0)


def _decorate(
    conn, task: dict[str, Any], *, include_comparison: bool = True
) -> dict[str, Any]:
    """给任务视图补上同源的锁定态与证据对照（页面 / 对照接口共用）。"""
    stream = streams_repo.get_stream(conn, task["stream_id"])
    live_cursor = int((stream or {}).get("apply_cursor") or 0)
    pending = task["status"] == PENDING
    task["live_cursor"] = live_cursor
    # pending 态下游标必须仍等于任务前值；锁定态由服务端判定，页面只负责展示。
    task["cursor_locked"] = pending and live_cursor == int(task["cursor_before"])
    task["cursor_switched"] = (
        task.get("cursor_after") is not None
        and int(task.get("cursor_after")) != int(task["cursor_before"])
    )
    if include_comparison:
        task["comparison"] = _comparison_for_task(conn, task)
    return task


def _comparison(evidence: Evidence, task_id: int | None = None) -> dict[str, Any]:
    """证据对照：账本侧 / Sink 侧候选分与是否冲突（只读，纯计算）。"""
    scores = side_scores(evidence)
    scores["task_id"] = task_id
    return scores


def _comparison_for_task(conn, task: dict[str, Any]) -> dict[str, Any]:
    """任务视图的对照：顶层是**当前**证据的两侧打分，并保留创建时快照。

    页面 / 对照接口 / 任务详情都读这一份，保证三处同源。
    """
    snapshot = task.get("evidence") if isinstance(task.get("evidence"), dict) else {}
    stored = _comparison(evidence_from_detail(snapshot), task.get("id"))
    stored["task_id"] = task.get("id")
    try:
        live = _comparison(rebuild_repo.scan_evidence(conn, task["stream_id"]), task.get("id"))
    except Exception:  # noqa: BLE001 - 对照只读，扫库失败不应拖垮任务视图
        live = None

    if live is None:
        current = dict(stored)
        current["scope"] = "snapshot"
        current["snapshot"] = stored
        current["live"] = None
        current["evidence_changed"] = None
        return current

    current = dict(live)
    current["scope"] = "live"
    current["snapshot"] = stored
    current["live"] = live
    current["evidence_changed"] = (
        stored["evidence_fingerprint"] != live["evidence_fingerprint"]
    )
    return current


def start_rebuild(stream_id: int) -> dict[str, Any]:
    """扫描证据并生成候选位点（只读证据，**绝不触碰 apply_cursor**）。"""
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        cursor_before = int(stream["apply_cursor"] or 0)

        evidence = rebuild_repo.scan_evidence(conn, stream_id)
        result = compute_candidate(evidence)
        fingerprint = evidence_fingerprint(evidence)

        status = PENDING if result.adoptable else "rejected"
        proposal = {
            "event": "scan_completed",
            "phase": "proposal",
            "action": "propose",
            "stream_id": stream_id,
            "cursor_before": cursor_before,
            "candidate": result.candidate,
            "evidence_fingerprint": fingerprint,
            "gate": result.gate,
            "adoptable": result.adoptable,
        }
        audit = {
            "proposal": proposal,
            "decision": None,
            "events": [proposal],
        }
        if not result.adoptable:
            # 门禁拒绝也是“决策”：显式记录游标未动，双相审计保持完整
            gate_decision = {
                "event": "rebuild_decided",
                "action": "reject",
                "action_by": "gate",
                "reason": "门禁未通过：" + "; ".join(result.reasons or [result.gate]),
                "cursor_after": cursor_before,
                "untouched": True,
                "cursor_drifted": False,
                "candidate": result.candidate,
                "evidence_fingerprint": fingerprint,
            }
            audit["decision"] = gate_decision
            audit["events"].append(
                {
                    "event": "gate_rejected",
                    "action": "reject",
                    "action_by": "gate",
                    "gate": result.gate,
                    "cursor_after": cursor_before,
                    "untouched": True,
                }
            )
        task_id = rebuild_repo.create_task(
            conn,
            stream_id=stream_id,
            status=status,
            gate=result.gate,
            adoptable=result.adoptable,
            candidate=result.candidate,
            cursor_before=cursor_before,
            scan_counts=evidence.scan_counts,
            evidence=result.detail,
            basis=result.basis,
            reasons=result.reasons,
            audit=audit,
        )
        # 关键：候选只落任务记录，不写游标。确认前游标保持任务前值。
        conn.commit()
        return _decorate(conn, rebuild_repo.get_task(conn, task_id))  # type: ignore[arg-type]
    finally:
        conn.close()


def _recheck(conn, task: dict[str, Any]) -> Evidence:
    """决策前重新扫库，防止任务挂起期间证据已变化。"""
    evidence = rebuild_repo.scan_evidence(conn, task["stream_id"])
    result = compute_candidate(evidence)
    expected = task.get("evidence_fingerprint") or (
        task.get("evidence") or {}
    ).get("evidence_fingerprint")
    if not result.adoptable or result.candidate != task["candidate_cursor"]:
        raise ValueError(
            "证据已变化或不再满足门禁（gate="
            f"{result.gate}），候选位点失效，请重新发起重建："
            + "; ".join(result.reasons or ["candidate changed"])
        )
    if expected and evidence_fingerprint(evidence) != expected:
        raise ValueError("证据指纹已变化，候选位点失效，请重新发起重建")
    return evidence


def _assert_pending(task: dict[str, Any]) -> None:
    if task["status"] != PENDING:
        raise ValueError(f"task already terminal: {task['status']}")


def confirm_rebuild(task_id: int) -> dict[str, Any]:
    """人工确认：复核通过后才写新位点，并追加 decision 审计。"""
    conn = connect()
    try:
        task = rebuild_repo.get_task(conn, task_id)
        if not task:
            raise ValueError("task not found")
        _assert_pending(task)
        if not task["adoptable"]:
            raise ValueError("candidate not adoptable")

        _recheck(conn, task)

        cursor_before = _live_cursor(conn, task["stream_id"])
        if cursor_before != int(task["cursor_before"]):
            raise ValueError(
                f"游标已被外部改写（任务前 {task['cursor_before']} → 当前 {cursor_before}），"
                "请重新发起重建"
            )
        new_cursor = int(task["candidate_cursor"])

        # 唯一的写游标动作，与审计在同一事务内提交
        streams_repo.set_cursor(conn, task["stream_id"], new_cursor)
        decision = {
            "event": "rebuild_decided",
            "action": "confirm",
            "cursor_after": new_cursor,
            "untouched": False,
            "candidate": new_cursor,
            "evidence_fingerprint": task.get("evidence_fingerprint"),
        }
        audit = task["audit"]
        audit["decision"] = decision
        audit.setdefault("events", []).append(
            {
                "event": "cursor_switched",
                "action": "confirm",
                "cursor_before": cursor_before,
                "cursor_after": new_cursor,
                "untouched": False,
                "decided_by": "manual_confirm",
            }
        )
        rebuild_repo.decide_task(
            conn,
            task_id=task_id,
            status="confirmed",
            cursor_after=new_cursor,
            audit=audit,
        )
        conn.commit()
        return _decorate(conn, rebuild_repo.get_task(conn, task_id))  # type: ignore[arg-type]
    finally:
        conn.close()


def reject_rebuild(task_id: int, reason: str = "") -> dict[str, Any]:
    """人工拒绝：**绝不改动游标**，只把任务关闭并追加 decision 审计。"""
    conn = connect()
    try:
        task = rebuild_repo.get_task(conn, task_id)
        if not task:
            raise ValueError("task not found")
        _assert_pending(task)

        cursor_now = _live_cursor(conn, task["stream_id"])
        decision = {
            "event": "rebuild_decided",
            "action": "reject",
            "reason": reason or "operator rejected candidate",
            "cursor_after": cursor_now,
            "untouched": True,
            # 拒绝是“本服务不动游标”：记录实时值，并显式标出是否偏离任务前值
            "cursor_drifted": cursor_now != int(task["cursor_before"]),
            "candidate": task.get("candidate_cursor"),
            "evidence_fingerprint": task.get("evidence_fingerprint"),
        }
        audit = task["audit"]
        audit["decision"] = decision
        audit.setdefault("events", []).append(
            {
                "event": "manual_rejected",
                "action": "reject",
                "reason": decision["reason"],
                "cursor_after": cursor_now,
                "untouched": True,
            }
        )
        rebuild_repo.decide_task(
            conn,
            task_id=task_id,
            status="rejected_manual",
            cursor_after=cursor_now,
            audit=audit,
        )
        conn.commit()
        return _decorate(conn, rebuild_repo.get_task(conn, task_id))  # type: ignore[arg-type]
    finally:
        conn.close()


def get_comparison(task_id: int) -> dict[str, Any]:
    """证据对照只读接口：按 task_id 返回两侧候选分与冲突判定。

    ``snapshot`` 来自任务创建时的持久化证据，``live`` 为当前扫库结果，
    二者使用同一个纯函数计算，页面直接复用本返回。
    """
    conn = connect()
    try:
        task = rebuild_repo.get_task(conn, task_id)
        if not task:
            raise ValueError("task not found")
        return {
            "task_id": task_id,
            "stream_id": task["stream_id"],
            "status": task["status"],
            "cursor_before": task["cursor_before"],
            "cursor_after": task["cursor_after"],
            "candidate_cursor": task["candidate_cursor"],
            "live_cursor": _live_cursor(conn, task["stream_id"]),
            **(_comparison_for_task(conn, task)),
        }
    finally:
        conn.close()


def list_tasks(stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return [
            _decorate(conn, t, include_comparison=False)
            for t in rebuild_repo.list_tasks(conn, stream_id)
        ]
    finally:
        conn.close()


def get_task(task_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        task = rebuild_repo.get_task(conn, task_id)
        if not task:
            raise ValueError("task not found")
        return _decorate(conn, task)
    finally:
        conn.close()


# ---- 仅用于验收/演练的故障注入，不是主链能力 ----

def demo_tamper_cursor(stream_id: int, bad_cursor: int) -> dict[str, Any]:
    """把游标改成错误值（模拟 apply_cursor 丢失/被改坏）。"""
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        before = int(stream["apply_cursor"] or 0)
        streams_repo.set_cursor(conn, stream_id, int(bad_cursor))
        conn.commit()
        return {"stream_id": stream_id, "cursor_before": before, "cursor_after": int(bad_cursor)}
    finally:
        conn.close()


def demo_inject_sink_contradiction(stream_id: int, phantom_seq: int) -> dict[str, Any]:
    """造一份 Sink/账本矛盾数据：插入账本中不存在的 Sink 行（触发 G2）。"""
    conn = connect()
    try:
        stream = streams_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        entity_key = f"{DEMO_ENTITY_PREFIX}s{stream_id}"
        sink_repo.upsert(
            conn,
            stream_id=stream_id,
            entity_key=entity_key,
            payload={"demo": "phantom sink row without ledger evidence"},
            seq=int(phantom_seq),
            event_ts="1970-01-01T00:00:00Z",
        )
        conn.commit()
        return {
            "stream_id": stream_id,
            "entity_key": entity_key,
            "phantom_last_seq": int(phantom_seq),
        }
    finally:
        conn.close()


def demo_clear_injected(stream_id: int) -> dict[str, Any]:
    """清理演练注入的 Sink 矛盾行。"""
    conn = connect()
    try:
        cur = conn.execute(
            "DELETE FROM sink_records WHERE stream_id=? AND entity_key LIKE ?",
            (stream_id, f"{DEMO_ENTITY_PREFIX}%"),
        )
        conn.commit()
        return {"stream_id": stream_id, "deleted": cur.rowcount}
    finally:
        conn.close()
