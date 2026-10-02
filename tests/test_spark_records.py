from __future__ import annotations

from trajfoundry.spark.records import ParsedSnapshotRow


def test_parsed_snapshot_row_is_small_and_shuffle_serializable() -> None:
    row = ParsedSnapshotRow(
        source_ref="00/req_001.json",
        sha256="a" * 64,
        scope_kind="session",
        scope_id="session-1",
        session_id="session-1",
        user_id="user-1",
        thread_id="thread-1",
        captured_at="2026-09-16T00:00:00.000Z",
        endpoint="/v1/messages",
        snapshot_json=b'{"messages":[]}',
    )

    assert row.as_tuple() == (
        "00/req_001.json",
        "a" * 64,
        "session",
        "session-1",
        "session-1",
        "user-1",
        "thread-1",
        "2026-09-16T00:00:00.000Z",
        "/v1/messages",
        b'{"messages":[]}',
    )
