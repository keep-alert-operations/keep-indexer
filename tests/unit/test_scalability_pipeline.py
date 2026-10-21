from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from appchi.search.projection import Projection, build_document
from appchi.search.reconciliation import (
    DEVELOPMENT_DOCUMENT_BYTES,
    document_allocation,
    publish_page,
)
from appchi.search.writer import ProjectionWriter
from appchi.shared.errors import AppError


def snapshot(notes=()):
    now = datetime(2026, 10, 6, tzinfo=UTC).isoformat()
    return {
        "id": "a",
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
        "time_created": now,
        "first_received_at": now,
        "timestamp": now,
        "occurrence_id": "occ",
        "occurrence_number": "1",
        "opened_at": now,
        "resolved_at": None,
        "acknowledged_by": None,
        "acknowledged_at": None,
        "assignee_id": None,
        "active_incident_id": None,
        "incident_ids": [],
        "notes": list(notes),
        "revision": "1",
        "live": True,
    }


def visible(items):
    return {
        "hits": {
            "hits": [
                {"_id": item.alert_id, "_version": item.version, "_source": build_document(item)}
                for item in items
            ]
        }
    }


def test_visible_identical_page_has_no_write_or_refresh():
    items = [Projection("tenant", 1, str(i), 1, {"deleted": True}) for i in range(50)]
    client = Mock()
    client.search.return_value = visible(items)
    writer = ProjectionWriter(client, "index", "epoch", max_document_bytes=100000)
    proof = writer.publish(
        items,
        guard=lambda *args: None,
        covered_by_newer=lambda *args: False,
    )
    assert len(proof.covered) == 50
    assert writer.written == 0
    client.bulk.assert_not_called()
    assert client.search.call_count == 1


def test_bulk_waits_for_one_refresh_for_fifty_documents():
    items = [Projection("tenant", 1, str(i), 1, {"deleted": True}) for i in range(50)]
    client = Mock()
    client.search.side_effect = [{"hits": {"hits": []}}, visible(items)]
    client.bulk.return_value = {"items": [{"index": {"status": 201}} for _ in items]}
    writer = ProjectionWriter(client, "index", "epoch", max_document_bytes=100000)
    proof = writer.publish(
        items,
        guard=lambda *args: None,
        covered_by_newer=lambda *args: False,
    )
    assert len(proof.covered) == 50
    assert writer.written == 50
    client.bulk.assert_called_once()
    assert len(client.bulk.call_args.kwargs["operations"]) == 100
    assert client.bulk.call_args.kwargs["refresh"] == "wait_for"


def test_supported_notes_above_one_megabyte_remain_searchable():
    notes = [" ".join(f"n{i:03d}_{j:04d}" for j in range(900)) for i in range(120)]
    item = Projection("tenant", 1, "a", 1, snapshot(notes))
    document = build_document(item)
    assert "n119_0899" in document["search_tokens"]
    client = Mock()
    client.search.side_effect = [{"hits": {"hits": []}}, visible([item])]
    client.bulk.return_value = {"items": [{"index": {"status": 201}}]}
    assert ProjectionWriter(client, "index", "epoch", max_document_bytes=2 * 1024 * 1024).publish(
        [item],
        guard=lambda *args: None,
        covered_by_newer=lambda *args: False,
    ).covered == (("tenant", 1, "a", 1),)
    assert (
        client.bulk.call_args.kwargs["operations"][1]["search_tokens"] == document["search_tokens"]
    )


def test_document_allocation_is_explicit_outside_development():
    assert (
        document_allocation(SimpleNamespace(environment="development"), None)
        == DEVELOPMENT_DOCUMENT_BYTES
    )
    for value in (None, {}, {"search_max_document_bytes": True}, {"search_max_document_bytes": 0}):
        with pytest.raises(AppError, match="") as failure:
            document_allocation(SimpleNamespace(environment="production"), value)
        assert failure.value.code == "search_document_capacity_profile_required"
    assert (
        document_allocation(
            SimpleNamespace(environment="production"), {"search_max_document_bytes": 2000000}
        )
        == 2000000
    )


def test_publish_page_splits_by_bytes_and_does_not_advance_over_tail(monkeypatch):
    from appchi.search import reconciliation

    monkeypatch.setattr(reconciliation, "BULK_BYTES", 2500)
    items = [Projection("tenant", 1, str(i), 1, {"deleted": True}) for i in range(8)]
    writer = Mock()
    monkeypatch.setattr(reconciliation, "ProjectionWriter", writer)
    selected = publish_page(Mock(), Mock(), "index", "epoch", items)
    assert 1 < len(selected) < len(items)
    assert selected == items[: len(selected)]
    writer.return_value.publish.assert_called_once()
    assert writer.return_value.publish.call_args.args[0] == selected


def test_failed_tenant_gets_retry_without_advancing_watermark(monkeypatch):
    from appchi.search import reconciliation, control

    queries = []

    class Connection:
        def execute(self, sql, params=()):
            queries.append((sql, params))
            if "SELECT ready" in sql:
                return Mock(fetchone=lambda: {"ready": True})
            if "RETURNING r.tenant_id" in sql:
                return Mock(fetchone=lambda: {"tenant_id": "tenant", "index_epoch": "epoch"})
            return Mock()

    @contextmanager
    def transaction():
        yield Connection()

    @contextmanager
    def lease(*args):
        yield "physical-index"

    db = SimpleNamespace(transaction=transaction)
    monkeypatch.setattr(control, "writer_lease", lease)
    monkeypatch.setattr(
        reconciliation, "reconcile_page", Mock(side_effect=RuntimeError("private details"))
    )
    service = reconciliation.ProjectionService.__new__(reconciliation.ProjectionService)
    service.db, service.settings, service.client, service.max_document_bytes = (
        db,
        Mock(),
        Mock(),
        2000000,
    )
    assert service.reconcile() == 0
    failure = next((sql, params) for sql, params in queries if "last_error=%s" in sql)
    assert failure[1][0] == "search_reconciliation_failed"
    assert "retry_after=clock_timestamp()+interval '5 seconds'" in failure[0]
    assert "lease_owner=%s" in failure[0]
    assert not any("UPDATE tenant_view_state" in sql for sql, _ in queries)
    assert any("SKIP LOCKED" in sql for sql, _ in queries)
