"""Opt-in tests against a real isolated Elasticsearch service; no substitute."""

from datetime import datetime, timezone
import os
from uuid import uuid4

from elasticsearch import Elasticsearch
import pytest

from appchi.cel import compile_expression
from appchi.search import Projection, SearchReader, SearchScope, SearchState, index_mapping
from appchi.search.writer import ProjectionWriter

pytestmark = pytest.mark.compatibility


@pytest.fixture
def real_search():
    endpoint = os.environ.get("APPCHI_TEST_ELASTICSEARCH_URL")
    if not endpoint:
        pytest.skip("Set APPCHI_TEST_ELASTICSEARCH_URL to an isolated real Elasticsearch service")
    ca = os.environ.get("APPCHI_TEST_ELASTICSEARCH_CA")
    api_key = os.environ.get("APPCHI_TEST_ELASTICSEARCH_API_KEY")
    client = Elasticsearch(
        endpoint,
        request_timeout=10,
        **({"ca_certs": ca} if ca else {}),
        **({"api_key": api_key} if api_key else {}),
    )
    index = "appchi-test-" + uuid4().hex
    client.indices.create(index=index, mappings=index_mapping())
    yield client, index
    client.indices.delete(index=index)
    client.close()


def test_real_native_es_equivalence_pit_and_tombstone(real_search, cipher):
    client, index = real_search
    now = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)
    tenant = str(uuid4())
    ids = [str(uuid4()), str(uuid4())]
    base = {
        "tenant_id": tenant,
        "schema_version": "v2",
        "key_field": "key",
        "operator": str(uuid4()),
        "application": "app",
        "component": "cpu",
        "site": "test",
        "impact": "",
        "runbook_url": "https://example.org",
        "severity": "critical",
        "status": "firing",
        "suppressed": False,
        "acknowledged": False,
        "time_created": now.isoformat(),
        "first_received_at": now.isoformat(),
        "timestamp": now.isoformat(),
        "occurrence_number": 1,
        "incident_ids": [],
        "notes": ["Straße"],
    }
    snapshots = [
        dict(base, id=ids[0], node_name=None, message="😀" * 8192),
        dict(base, id=ids[1], node_name="a*?", message="case DIFFERENT"),
    ]
    items = [Projection(tenant, i + 1, ids[i], 1, snapshots[i]) for i in range(2)]
    writer = ProjectionWriter(client, index, "test-epoch", max_document_bytes=1024 * 1024)

    def guard(*args):
        pass  # immutable test fixtures; production guard must consult PG

    proof = writer.publish(items, guard=guard, covered_by_newer=lambda *args: False)
    assert len(proof.covered) == 2
    state = SearchState("test-epoch", 2, 2, 0, now.isoformat())
    service = SearchReader(
        client,
        index,
        cipher,
        clock=lambda: now,
        state=lambda tenant: state,
        authorize=lambda scope: None,
        retained_ids=lambda tenant, found: set(found),
    )
    scope = SearchScope(tenant, "test-session", "test-deploy")
    expressions = [
        "alert.node_name == null",
        '!alert.node_name.contains("x")',
        'alert.node_name.contains("*?")',
        'alert.message.contains("😀")',
        'alert.time_created >= now() - duration("1s")',
        "alert.occurrence_number in [1,2]",
    ]
    for source in expressions:
        expr = compile_expression(source)
        expected = {doc["id"] for doc in snapshots if expr.evaluate(doc, now)}
        page = service.page(expr, scope)
        assert {doc["id"] for doc in page.items} == expected
        assert page.total == len(expected)
    page = service.page(compile_expression("true"), scope, page_size=1, q="STRASSE")
    assert page.total == 2 and page.next_cursor
    second = service.page(
        compile_expression("true"), scope, page_size=1, q="STRASSE", cursor=page.next_cursor
    )
    assert page.items[0]["id"] != second.items[0]["id"]
    tombstone = Projection(tenant, 3, ids[0], 2, {"deleted": True})
    writer.publish([tombstone], guard=guard, covered_by_newer=lambda *args: False)
    writer.publish(
        [items[0]], guard=guard, covered_by_newer=lambda item, doc: doc.get("deleted") is True
    )
    page = service.page(compile_expression("true"), scope)
    assert {doc["id"] for doc in page.items} == {ids[1]}
