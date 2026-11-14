"""failover_offset_rebuild 端到端闭环测试，全程本地临时 SQLite。"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def db_env(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setenv("DATA_DIR", str(data_dir))

    import app.config as config
    import app.db as db

    importlib.reload(config)
    monkeypatch.setattr(db, "DB_PATH", data_dir / "app.db")
    monkeypatch.setattr(db, "DATA_DIR", data_dir)

    from app import seed
    from app.repositories import cursors as cursor_repo
    from app.repositories import sink as sink_repo
    from app.services import apply_service, failover_rebuild_service as svc

    seed.init_db()
    return {
        "cursor_repo": cursor_repo,
        "sink_repo": sink_repo,
        "apply_service": apply_service,
        "svc": svc,
    }


def _stream_cursor(stream_id: int) -> int:
    from app.db import connect
    from app.repositories import streams as streams_repo

    conn = connect()
    try:
        return int(streams_repo.get_stream(conn, stream_id)["apply_cursor"])
    finally:
        conn.close()


def test_rebuild_success_after_corrupt_cursor(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    # 1. 正常跑一轮：Orders CDC seq 1..5 全部应用，游标到 5
    run = apply_service.run_apply(1)
    assert run["status"] == "succeeded"
    assert _stream_cursor(1) == 5

    # 2. 故障注入：游标被改坏（清零瞎跑的典型反面场景）
    svc.demo_tamper_cursor(1, 0)
    assert _stream_cursor(1) == 0

    # 3. 发起重建：只扫证据，不动游标
    task = svc.start_rebuild(1)
    assert task["status"] == "awaiting_confirmation"
    assert task["adoptable"] is True
    assert task["candidate_cursor"] == 5
    assert _stream_cursor(1) == 0  # 未确认前游标保持坏值
    assert task["scan_counts"]["applied_seqs_rows"] == 5
    assert task["scan_counts"]["sink_records_rows"] >= 1
    assert any("候选位点" in b for b in task["basis"])

    # 4. 人工确认后才切换，且留审计
    decided = svc.confirm_rebuild(task["id"])
    assert decided["status"] == "confirmed"
    assert decided["cursor_before"] == 0
    assert decided["cursor_after"] == 5
    assert _stream_cursor(1) == 5
    events = [e["event"] for e in decided["audit"]["events"]]
    assert "scan_completed" in events and "cursor_switched" in events


def test_rebuild_rejected_on_sink_ledger_conflict(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    assert _stream_cursor(1) == 5
    svc.demo_tamper_cursor(1, 99)

    # 造矛盾：Sink 插入账本中不存在的 last_seq=99
    svc.demo_inject_sink_contradiction(1, 99)

    task = svc.start_rebuild(1)
    # 必须拒绝切换，且不允许标成成功
    assert task["status"] == "rejected"
    assert task["adoptable"] is False
    assert any("G2" in r for r in task["reasons"])
    assert _stream_cursor(1) == 99  # 坏游标原封不动

    # 被拒任务不允许再确认
    with pytest.raises(ValueError):
        svc.confirm_rebuild(task["id"])

    # 清理矛盾数据后重新发起，应能成功恢复到 5
    svc.demo_clear_injected(1)
    task2 = svc.start_rebuild(1)
    assert task2["adoptable"] is True
    decided = svc.confirm_rebuild(task2["id"])
    assert decided["status"] == "confirmed"
    assert decided["cursor_after"] == 5
    assert _stream_cursor(1) == 5


def test_manual_reject_keeps_cursor_untouched(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(2)  # Inventory seq 1..3
    svc.demo_tamper_cursor(2, 0)

    task = svc.start_rebuild(2)
    assert task["candidate_cursor"] == 3
    rejected = svc.reject_rebuild(task["id"], reason="人工暂不切换")
    assert rejected["status"] == "rejected_manual"
    assert rejected["cursor_after"] == 0
    assert _stream_cursor(2) == 0

    # 终态任务不能重复决策
    with pytest.raises(ValueError):
        svc.confirm_rebuild(task["id"])


def test_stale_confirmation_blocked_when_evidence_changes(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 0)
    task = svc.start_rebuild(1)
    assert task["adoptable"] is True

    # 挂起期间证据被污染，确认时必须重新复核并拦下
    svc.demo_inject_sink_contradiction(1, 99)
    with pytest.raises(ValueError):
        svc.confirm_rebuild(task["id"])
    assert _stream_cursor(1) == 0


def test_task_persistence_and_listing(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 0)
    created = svc.start_rebuild(1)
    svc.confirm_rebuild(created["id"])

    # 重新开连接读取，验证持久化
    tasks = svc.list_tasks(1)
    assert len(tasks) == 1
    stored = tasks[0]
    assert stored["id"] == created["id"]
    assert stored["status"] == "confirmed"
    assert stored["evidence"]["applied_seqs"] == [1, 2, 3, 4, 5]
    assert svc.get_task(created["id"])["candidate_cursor"] == 5


# ---- 回归：未确认/已拒绝不得改写游标；双相审计与证据对照同源 ----


def test_start_rebuild_never_switches_cursor_and_writes_proposal_audit(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 0)

    task = svc.start_rebuild(1)
    assert task["status"] == "awaiting_confirmation"
    assert _stream_cursor(1) == 0  # 未确认前游标保持任务前值
    assert task["cursor_locked"] is True
    assert task["cursor_switched"] is False
    assert task["cursor_after"] == 0

    proposal = task["audit"]["proposal"]
    assert proposal["event"] == "scan_completed"
    assert proposal["phase"] == "proposal"
    assert proposal["cursor_before"] == 0
    assert proposal["candidate"] == 5
    assert proposal["gate"] == "ok"
    assert proposal["evidence_fingerprint"] == task["evidence_fingerprint"]
    assert task["audit"]["decision"] is None


def test_reject_never_moves_cursor_and_appends_decision_audit(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(2)
    svc.demo_tamper_cursor(2, 7)  # 坏游标，非 0，确认拒绝后不被改成候选

    task = svc.start_rebuild(2)
    assert task["candidate_cursor"] == 3
    assert _stream_cursor(2) == 7

    rejected = svc.reject_rebuild(task["id"], reason="人工暂不切换")
    assert rejected["status"] == "rejected_manual"
    assert _stream_cursor(2) == 7  # 关键：拒绝绝不把游标改成候选
    assert rejected["cursor_after"] == 7
    assert rejected["cursor_switched"] is False

    decision = rejected["audit"]["decision"]
    assert decision["action"] == "reject"
    assert decision["untouched"] is True
    assert decision["cursor_after"] == 7
    assert [e["event"] for e in rejected["audit"]["events"]] == [
        "scan_completed",
        "manual_rejected",
    ]


def test_confirm_appends_decision_audit(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 0)
    task = svc.start_rebuild(1)
    assert _stream_cursor(1) == 0

    decided = svc.confirm_rebuild(task["id"])
    decision = decided["audit"]["decision"]
    assert decision["action"] == "confirm"
    assert decision["untouched"] is False
    assert decision["cursor_after"] == 5
    assert decided["cursor_after"] == 5
    assert _stream_cursor(1) == 5
    events = [e["event"] for e in decided["audit"]["events"]]
    assert events == ["scan_completed", "cursor_switched"]


def test_rejected_task_never_switches_cursor(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 99)
    svc.demo_inject_sink_contradiction(1, 99)

    task = svc.start_rebuild(1)
    assert task["status"] == "rejected"
    assert task["candidate_cursor"] == 5
    assert _stream_cursor(1) == 99  # 游标保持任务前值，不切到任一侧
    assert task["cursor_locked"] is False  # 终态，锁定态不再适用
    assert task["cursor_switched"] is False


def test_comparison_endpoint_reports_side_scores_and_conflict(db_env):
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 0)
    task = svc.start_rebuild(1)

    ok = svc.get_comparison(task["id"])
    assert ok["task_id"] == task["id"]
    assert ok["ledger_side"]["candidate_score"] == 5
    assert ok["sink_side"]["candidate_score"] == 5
    assert ok["conflict"] is False
    assert ok["agree"] is True
    assert ok["evidence_changed"] is False
    assert ok["scope"] == "live"
    # 与任务视图同源
    assert ok["evidence_fingerprint"] == task["evidence"]["evidence_fingerprint"]
    assert task["comparison"]["ledger_side"]["candidate_score"] == ok["ledger_side"]["candidate_score"]

    # 任务挂起期间注入矛盾，对照必须报冲突且标记证据已变化
    svc.demo_inject_sink_contradiction(1, 99)
    conflict = svc.get_comparison(task["id"])
    assert conflict["conflict"] is True
    assert conflict["agree"] is False
    assert conflict["sink_side"]["candidate_score"] == 99
    assert 99 in conflict["sink_side"]["unexplained_seqs"]
    assert conflict["evidence_changed"] is True
    # 任务创建时快照仍可追溯（候选为干净的 5）
    assert conflict["snapshot"]["sink_side"]["candidate_score"] == 5
    assert conflict["snapshot"]["conflict"] is False


def test_stale_task_blocked_by_fingerprint_even_if_candidate_same(db_env):
    """证据变化但候选位点数值不变时，也必须拒绝确认。"""
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 0)
    task = svc.start_rebuild(1)
    assert task["candidate_cursor"] == 5

    # 新增 seq 7（6 缺口，故候选仍为 5），但证据指纹必然变化
    from app.db import connect

    conn = connect()
    try:
        conn.execute(
            "INSERT INTO applied_seqs(stream_id, seq, run_id, applied_at) VALUES (?,?,?,?)",
            (1, 7, 999, "2026-11-01T00:00:00Z"),
        )
        conn.execute(
            """
            INSERT INTO change_ledger(stream_id, run_id, seq, op, entity_key, event_ts,
                                      payload_json, disposition)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (1, 999, 7, "upsert", "ORD-9999", "2026-11-01T00:00:00Z", "{}", "applied"),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(ValueError):
        svc.confirm_rebuild(task["id"])
    assert _stream_cursor(1) == 0

    # 对照接口也应报告证据已变化
    assert svc.get_comparison(task["id"])["evidence_changed"] is True


def test_gate_rejected_task_records_decision_audit_untouched(db_env):
    """门禁拒绝也写 decision 审计；游标保持任务前值。"""
    svc = db_env["svc"]
    apply_service = db_env["apply_service"]

    apply_service.run_apply(1)
    svc.demo_tamper_cursor(1, 99)
    svc.demo_inject_sink_contradiction(1, 99)

    task = svc.start_rebuild(1)
    assert task["status"] == "rejected"
    decision = task["audit"]["decision"]
    assert decision["action"] == "reject"
    assert decision["action_by"] == "gate"
    assert decision["untouched"] is True
    assert decision["cursor_after"] == 99
    assert task["cursor_after"] == 99
    assert _stream_cursor(1) == 99
    assert task["audit"]["proposal"]["phase"] == "proposal"
    assert [e["event"] for e in task["audit"]["events"]] == [
        "scan_completed",
        "gate_rejected",
    ]


def test_comparison_reports_insufficient_evidence(db_env):
    svc = db_env["svc"]

    task = svc.start_rebuild(2)  # 无任何应用/ Sink 证据
    assert task["status"] == "rejected"
    assert task["gate"] == "G1"
    cmp = svc.get_comparison(task["id"])
    assert cmp["sufficient"] is False
    assert cmp["ledger_side"]["candidate_score"] == 0
    assert cmp["sink_side"]["candidate_score"] == 0
    assert _stream_cursor(2) == 0
