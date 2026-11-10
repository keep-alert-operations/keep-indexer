from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from keep.search.projection import Projection, build_document
from keep.search.reconciliation import (
    DEVELOPMENT_DOCUMENT_BYTES,
    document_allocation,
    publish_page,
)
from keep.search.writer import ProjectionWriter
from keep.shared.errors import AppError, unavailable


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


def test_write_is_reported_when_verification_after_bulk_fails(monkeypatch):
    from keep.search import reconciliation

    items = [Projection("tenant", 1, str(i), 1, {"deleted": True}) for i in range(3)]
    client = Mock()
    client.search.side_effect = [{"hits": {"hits": []}}, RuntimeError("degraded")]
    client.bulk.return_value = {"items": [{"index": {"status": 201}} for _ in items]}
    writer = ProjectionWriter(client, "index", "epoch", max_document_bytes=100000)
    with pytest.raises(AppError) as failure:
        writer.publish(items, guard=lambda *args: None, covered_by_newer=lambda *args: False)
    assert failure.value.code == "projection_unavailable"
    assert writer.written == 3
    failing = Mock()
    failing.return_value.written = 3
    failing.return_value.publish.side_effect = unavailable("projection_unavailable")
    monkeypatch.setattr(reconciliation, "ProjectionWriter", failing)
    announced = Mock()
    with pytest.raises(AppError):
        publish_page(Mock(), Mock(), "index", "epoch", items, on_write=announced)
    announced.assert_called_once_with()


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
    from keep.search import reconciliation

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
    from keep.search import reconciliation, control

    queries = []

    class Connection:
        def execute(self, sql, params=()):
            queries.append((sql, params))
            if "AS due_in" in sql:
                return Mock(fetchone=lambda: {"ready": True, "unregistered": False, "due_in": 0})
            if "SELECT ready" in sql:
                return Mock(fetchone=lambda: {"ready": True})
            if "RETURNING r.tenant_id" in sql:
                return Mock(fetchone=lambda: {"tenant_id": "tenant", "index_epoch": "epoch"})
            return Mock()

    @contextmanager
    def transaction(*args, **kwargs):
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


def test_one_bulk_and_one_refresh_wait_cover_the_writer_document_limit():
    items = [Projection(f"tenant{i % 7}", 1, str(i), 1, {"deleted": True}) for i in range(500)]
    client = Mock()
    client.search.side_effect = [{"hits": {"hits": []}}, visible(items)]
    client.bulk.return_value = {"items": [{"index": {"status": 201}} for _ in items]}
    proof = ProjectionWriter(client, "index", "epoch", max_document_bytes=100000).publish(
        items,
        guard=lambda *args: None,
        covered_by_newer=lambda *args: False,
    )
    assert len(proof.covered) == 500
    client.bulk.assert_called_once()
    assert len(client.bulk.call_args.kwargs["operations"]) == 1000
    assert client.bulk.call_args.kwargs["refresh"] == "wait_for"
    extra = [*items, Projection("tenant", 1, "500", 1, {"deleted": True})]
    with pytest.raises(AppError) as failure:
        ProjectionWriter(Mock(), "index", "epoch", max_document_bytes=100000).publish(
            extra, guard=lambda *args: None, covered_by_newer=lambda *args: False
        )
    assert failure.value.code == "projection_batch_limit"


def test_publish_page_selects_up_to_500_documents_within_4_mib(monkeypatch):
    from keep.search import reconciliation

    writer = Mock()
    monkeypatch.setattr(reconciliation, "ProjectionWriter", writer)
    items = [Projection("tenant", 1, str(i), 1, {"deleted": True}) for i in range(600)]
    selected = publish_page(Mock(), Mock(), "index", "epoch", items)
    assert selected == items[:500]
    assert writer.return_value.publish.call_args.args[0] == items[:500]
    # Large documents still stop at 4 MiB, never past an unselected item.
    tokens = tuple(f"token{j:06d}" for j in range(8000))
    items = [
        Projection("tenant", 1, str(i), 1, {**snapshot(), "id": str(i)}, tokens) for i in range(100)
    ]
    selected = publish_page(Mock(), Mock(), "index", "epoch", items)
    assert 1 < len(selected) < 100 and selected == items[: len(selected)]
    total = sum(len(reconciliation.canonical(build_document(i))) + 512 for i in selected)
    assert total <= reconciliation.BULK_BYTES


def test_project_many_stops_assembling_at_the_document_limit(monkeypatch):
    from keep.search import control, reconciliation

    @contextmanager
    def lease(db, settings):
        yield "index"

    monkeypatch.setattr(control, "writer_lease", lease)
    assembled = []

    def page(conn, tenant, *, alert_ids, max_document_bytes):
        rows = [Projection(tenant, 1, alert, 1, {"deleted": True}) for alert in alert_ids]
        assembled.append(tenant)
        return rows

    monkeypatch.setattr(reconciliation, "current_page", page)
    published = []

    def publish(db, client, physical_index, epoch, items, **kwargs):
        published.append(items)
        return items[:500]

    monkeypatch.setattr(reconciliation, "publish_page", publish)

    @contextmanager
    def transaction(*args, **kwargs):
        yield SimpleNamespace(
            execute=lambda *a: SimpleNamespace(fetchone=lambda: {"index_epoch": "epoch"})
        )

    service = reconciliation.ProjectionService.__new__(reconciliation.ProjectionService)
    service.db = SimpleNamespace(transaction=transaction)
    service.settings, service.client, service.max_document_bytes = None, Mock(), 100000
    requests = [(f"t{t}", f"{t}-{a}") for t in range(4) for a in range(200)]
    covered = service.project_many(requests)
    # Three tenants fill 600 >= 500 candidates; the fourth is never read.
    assert assembled == ["t0", "t1", "t2"]
    assert len(published) == 1 and len(covered) == 500
