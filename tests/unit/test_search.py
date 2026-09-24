from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from appchi.cel import compile_expression
from appchi.search import (
    Projection,
    ProjectionWriter,
    SearchReader,
    SearchScope,
    SearchState,
    build_document,
    index_mapping,
)
from appchi.search.reader import sort_spec
from appchi.search.text import query_tokens, source_tokens
from appchi.shared.errors import AppError

NOW = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)


def snapshot(alert_id="a", **changes):
    result = {
        "id": alert_id,
        "tenant_id": "tenant",
        "schema_version": "v1",
        "key_field": "key",
        "operator": "legacy",
        "application": "app",
        "component": "cpu",
        "node_name": None,
        "site": None,
        "impact": None,
        "runbook_url": None,
        "message": "Disk FAILED",
        "severity": "critical",
        "status": "firing",
        "suppressed": False,
        "acknowledged": False,
        "time_created": NOW.isoformat(),
        "first_received_at": NOW.isoformat(),
        "timestamp": NOW.isoformat(),
        "occurrence_id": "occ",
        "occurrence_number": "1",
        "opened_at": NOW.isoformat(),
        "resolved_at": None,
        "acknowledged_by": None,
        "acknowledged_at": None,
        "assignee_id": None,
        "active_incident_id": None,
        "incident_ids": [],
        "notes": ["Straße details"],
        "revision": "1",
        "live": True,
    }
    result.update(changes)
    return result


def document(alert_id="a", **changes):
    return build_document(Projection("tenant", 1, alert_id, 1, snapshot(alert_id, **changes)))


def test_strict_mapping_and_lossless_projection():
    mapping = index_mapping()
    assert mapping["dynamic"] == "strict"
    assert mapping["properties"]["message"]["type"] == "wildcard"
    assert mapping["properties"]["time_created"]["type"] == "long"
    doc = document(message="😀" * 8192)
    assert doc["message"] == "😀" * 8192
    assert doc["occurrence_number"] == 1
    assert doc["display"]["occurrence_number"] == "1"
    assert doc["missing_node_name"] is True
    assert "strasse" in doc["search_tokens"]
    assert doc["display"]["timestamp"] == NOW.isoformat()


def test_tombstone_has_no_payload_or_actor_and_stable_identity():
    item = Projection("tenant", 3, "a", 3, snapshot(live=False))
    assert build_document(item) == {
        "id": "a",
        "tenant_id": "tenant",
        "projection_version": 3,
        "deleted": True,
    }


def test_token_profile_casefold_without_normalization_and_length():
    assert query_tokens("STRASSE disk disk") == ["disk", "strasse"]
    assert query_tokens("É") != query_tokens("e\u0301")
    assert query_tokens("ab_cd 1α") == ["1α", "ab_cd"]
    assert source_tokens(["a" * 10000, "ok"]) == ["ok"]
    with pytest.raises(AppError):
        query_tokens("!!!")
    with pytest.raises(AppError):
        query_tokens("a" * 257)


def test_null_last_and_clear_last_both_sort_directions():
    for direction in ("asc", "desc"):
        assert sort_spec("node_name", direction)[0] == {"missing_node_name": "asc"}
        assert sort_spec("severity", direction)[0] == {"severity_clear_last": "asc"}
        assert sort_spec("severity", direction)[-1] == {"id": "asc"}
    with pytest.raises(AppError):
        sort_spec("notes", "asc")


@pytest.fixture
def reader(cipher):
    client = Mock()
    client.open_point_in_time.return_value = {"id": "pit-1", "_shards": {"failed": 0}}
    state = SearchState("epoch", 2, 1, 0, NOW.isoformat())
    states = Mock(return_value=state)
    auth = Mock()
    reader = SearchReader(
        client,
        "test-index",
        cipher,
        clock=lambda: NOW,
        state=states,
        authorize=auth,
        retained_ids=lambda tenant, ids: set(ids),
    )
    return reader, client, states, auth


def page_response(ids, total=2):
    return {
        "pit_id": "pit-latest",
        "timed_out": False,
        "_shards": {"failed": 0},
        "hits": {
            "total": {"value": total, "relation": "eq"},
            "hits": [{"_id": aid, "_source": document(aid), "sort": [1, aid, 0]} for aid in ids],
        },
    }


def test_cursor_binds_scope_query_epoch_and_preserves_pre_pit_watermark(reader):
    service, client, states, auth = reader
    scope = SearchScope("tenant", "session:user", "deploy")
    expr = compile_expression("true")
    client.search.return_value = page_response(["a"])
    page = service.page(expr, scope, page_size=1)
    assert page.total == 2 and page.next_cursor
    assert page.watermark["searchable_seq"] == 1
    assert page.updating is True
    client.search.return_value = page_response(["b"])
    resumed = service.page(expr, scope, page_size=1, cursor=page.next_cursor)
    assert resumed.evaluated_at == page.evaluated_at
    assert client.search.call_args.kwargs["search_after"] == [1, "a", 0]
    assert client.search.call_args.kwargs["pit"]["id"] == "pit-latest"
    assert resumed.next_cursor is None
    with pytest.raises(AppError) as failure:
        service.page(expr, replace(scope, tenant_id="other"), cursor=page.next_cursor)
    assert failure.value.code == "invalid_cursor"
    with pytest.raises(AppError) as failure:
        service.page(expr, scope, q="different", cursor=page.next_cursor)
    assert failure.value.code == "query_changed"
    states.return_value = replace(states.return_value, index_epoch="new")
    with pytest.raises(AppError) as failure:
        service.page(expr, scope, cursor=page.next_cursor)
    assert failure.value.code == "index_changed"
    assert auth.call_count >= 5


def test_purged_page_keeps_last_examined_cursor_and_marks_total_unavailable(reader):
    service, client, states, _ = reader
    service.retained_ids = lambda tenant, ids: set()
    client.search.return_value = page_response(["a"])
    page = service.page(
        compile_expression("true"), SearchScope("tenant", "session", "deploy"), page_size=1
    )
    assert page.items == [] and page.total is None and page.next_cursor
    assert page.updating


def test_partial_search_never_becomes_zero(reader):
    service, client, _, _ = reader
    client.search.return_value = {**page_response([], 0), "timed_out": True}
    with pytest.raises(AppError) as failure:
        service.page(compile_expression("true"), SearchScope("tenant", "session", "deploy"))
    assert failure.value.status == 503
    client.close_point_in_time.assert_called()


def test_expiry_absolute_not_extended_by_pages(reader):
    service, client, _, _ = reader
    client.search.return_value = page_response(["a"])
    scope = SearchScope("tenant", "session", "deploy")
    expr = compile_expression("true")
    page = service.page(expr, scope, page_size=1)
    service.clock = lambda: NOW + timedelta(minutes=5)
    with pytest.raises(AppError) as failure:
        service.page(expr, scope, cursor=page.next_cursor)
    assert failure.value.code == "cursor_expired"


def test_projection_requires_search_visibility_and_equal_version_identity():
    client = Mock()
    writer = ProjectionWriter(client, "index", "epoch", max_document_bytes=100000)
    item = Projection("tenant", 1, "a", 1, snapshot())
    client.bulk.return_value = {"items": [{"index": {"status": 201, "_shards": {"failed": 0}}}]}
    client.search.side_effect = [
        page_response([], 0),
        {"hits": {"hits": [{"_id": "a", "_version": 1, "_source": build_document(item)}]}},
    ]
    guard = Mock()
    proof = writer.publish([item], guard=guard, covered_by_newer=lambda item, source: False)
    assert proof.covered == (("tenant", 1, "a", 1),)
    assert client.bulk.call_args.kwargs["refresh"] == "wait_for"
    guard.assert_called_once()
    client.search.side_effect = None
    client.search.return_value = {"hits": {"hits": [{"_id": "a", "_version": 1, "_source": {}}]}}
    with pytest.raises(AppError) as failure:
        writer.publish([item], guard=guard, covered_by_newer=lambda item, source: False)
    assert failure.value.code == "projection_version_conflict"


def test_failed_projection_hole_has_no_visibility_proof():
    client = Mock()
    writer = ProjectionWriter(client, "index", "epoch", max_document_bytes=100000)
    client.search.return_value = page_response([], 0)
    client.bulk.return_value = {"items": [{"index": {"status": 429}}]}
    with pytest.raises(AppError) as failure:
        writer.publish(
            [Projection("tenant", 1, "a", 1, snapshot())],
            guard=lambda *args: None,
            covered_by_newer=lambda *args: False,
        )
    assert failure.value.status == 503
