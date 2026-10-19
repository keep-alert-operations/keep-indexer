"""Live, disposable diagnostics for initial development search activation.

This proves only the exercised application operations on this target. It cannot
prove infrastructure ownership, writer exclusivity, capacity or production readiness.
"""

from datetime import UTC, datetime
from hashlib import sha256
import os
from uuid import uuid4

from appchi.cel import compile_expression
from appchi.search import Projection, SearchReader, SearchScope, SearchState
from appchi.search.counts import CountReader, CountState
from appchi.search.projection import build_document, canonical, index_mapping
from appchi.search.rebuild import alias_target
from appchi.search.writer import ProjectionWriter
from appchi.shared.crypto import EnvelopeCipher
from appchi.shared.errors import unavailable


def target_identity(client):
    info = client.info()
    result = {
        "cluster_uuid": info.get("cluster_uuid"),
        "version": info.get("version", {}).get("number"),
    }
    if any(not isinstance(value, str) or value in {"", "_na_"} for value in result.values()):
        raise unavailable("bootstrap_target_unverified")
    return result


def owned_mapping(intent, purpose):
    mapping = index_mapping()
    mapping["_meta"]["initial_bootstrap"] = {
        "intent_sha256": sha256(canonical(intent)).hexdigest(),
        "purpose": purpose,
    }
    return mapping


def verify_mapping(client, physical, expected):
    observed = client.indices.get_mapping(index=physical)[physical]["mappings"]
    # ES may omit the default enabled=true _source setting. Reject all other
    # mapping changes, including runtime fields and field/source filtering.
    normalized = dict(observed)
    normalized.setdefault("_source", {"enabled": True})
    if normalized != expected:
        raise unavailable("bootstrap_mapping_mismatch")


def _expect_status(call, status):
    try:
        call()
    except Exception as error:
        if getattr(error, "status_code", None) == status:
            return
        raise
    raise unavailable("bootstrap_capability_failed")


def _check(condition):
    if not condition:
        raise unavailable("bootstrap_capability_failed")


def qualify(client, binding, intent):
    """Execute real operations; evidence is produced here, never accepted as JSON."""
    prefix = binding.index_prefix + "bootstrap-probe-" + intent["index_epoch"]
    first, second, alias = prefix + "-a", prefix + "-b", prefix + "-alias"
    expected = owned_mapping(intent, "disposable-probe")

    def cleanup():
        for name in (first, second):
            if client.indices.exists(index=name):
                verify_mapping(client, name, expected)
                aliases = client.indices.get_alias(index=name)[name].get("aliases", {})
                if set(aliases) - {alias}:
                    raise unavailable("bootstrap_probe_ownership_conflict")
                client.indices.delete(index=name)

    cleanup()  # Resume only resources carrying this exact durable intent.
    if client.indices.exists(index=alias) or client.indices.exists_alias(name=alias):
        raise unavailable("bootstrap_probe_ownership_conflict")
    started = datetime.now(UTC)
    try:
        client.indices.create(
            index=first, mappings=expected, settings=binding.index_settings, aliases={alias: {}}
        )
        client.indices.create(index=second, mappings=expected, settings=binding.index_settings)
        verify_mapping(client, first, expected)
        verify_mapping(client, second, expected)
        _exercise(client, first, binding.max_document_bytes, intent["index_epoch"])
        # One API request changes both alias memberships; inspect its real result.
        client.indices.update_aliases(
            actions=[
                {"remove": {"index": first, "alias": alias, "must_exist": True}},
                {"add": {"index": second, "alias": alias}},
            ]
        )
        _check(alias_target(client, alias) == second)
        observed = target_identity(client)
        _check(observed == intent["target"])
    finally:
        cleanup()  # Failure or uncertain cleanup must not activate the search gate.
    return {
        "profile": "appchi-development-bootstrap-probe-v1",
        "started_at": started.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "target": observed,
        "binding_sha256": binding.digest(),
        "mapping_sha256": sha256(canonical(index_mapping())).hexdigest(),
        "checks": [
            "strict-mapping",
            "external-version",
            "refresh-visibility",
            "native-cel-equivalence",
            "tenant-isolation",
            "unicode-tokenization",
            "pit-pagination",
            "exact-count-batch",
            "tombstone-replay",
            "atomic-alias-switch",
        ],
    }


def _exercise(client, index, max_bytes, epoch):
    now, tenant = datetime.now(UTC), str(uuid4())
    ids = [str(uuid4()) for _ in range(3)]
    base = {
        "tenant_id": tenant,
        "schema_version": "v2",
        "key_field": "key",
        "operator": str(uuid4()),
        "application": "bootstrap-probe",
        "component": "cpu",
        "site": "test",
        "impact": "",
        "runbook_url": "https://example.invalid",
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
    items = [
        Projection(tenant, position + 1, ids[position], 1, doc)
        for position, doc in enumerate(snapshots)
    ]
    other = str(uuid4())
    items.append(Projection(other, 1, ids[2], 1, dict(snapshots[1], id=ids[2], tenant_id=other)))
    writer = ProjectionWriter(client, index, epoch, max_document_bytes=max_bytes)

    # Only immutable synthetic fixtures enter this writer; this is not a runtime PG guard.
    def guard(*args):
        pass

    _check(
        len(writer.publish(items, guard=guard, covered_by_newer=lambda *args: False).covered) == 3
    )
    _expect_status(
        lambda: client.index(index=index, id="unknown-field", document={"unexpected": True}), 400
    )
    state = SearchState(epoch, 2, 2, 0, now.isoformat())
    reader = SearchReader(
        client,
        index,
        EnvelopeCipher({"probe": os.urandom(32)}, "probe"),
        clock=lambda: now,
        state=lambda _: state,
        authorize=lambda _: None,
        retained_ids=lambda _, found: set(found),
    )
    scope = SearchScope(tenant, "bootstrap-probe", "bootstrap-probe")
    sources = [
        "true",
        "alert.node_name == null",
        '!alert.node_name.contains("x")',
        'alert.node_name.contains("*?")',
        'alert.message.contains("😀")',
        'alert.time_created >= now() - duration("1s")',
        "alert.occurrence_number in [1,2]",
    ]
    expressions, counts = {}, {}
    for source in sources:
        expression = compile_expression(source)
        expressions[source] = expression
        expected = {doc["id"] for doc in snapshots if expression.evaluate(doc, now)}
        page = reader.page(expression, scope)
        _check({doc["id"] for doc in page.items} == expected and page.total == len(expected))
        counts[source] = len(expected)
    page = reader.page(expressions["true"], scope, page_size=1, q="STRASSE")
    _check(page.total == 2 and page.next_cursor is not None)
    second = reader.page(
        expressions["true"], scope, page_size=1, q="STRASSE", cursor=page.next_cursor
    )
    _check(len(second.items) == 1 and page.items[0]["id"] != second.items[0]["id"])
    if second.next_cursor:
        reader.dispose(second.next_cursor, scope)
    before = CountState(epoch, 2, 2, 0, now.isoformat(), 0, False, "probe")
    batch = CountReader(client, index).read(
        expressions, tenant, evaluated_at=now, before=before, revalidate=lambda: before
    )
    _check(batch.counts == counts)
    tombstone = Projection(tenant, 3, ids[0], 2, {"deleted": True})
    writer.publish([tombstone], guard=guard, covered_by_newer=lambda *args: False)
    _expect_status(
        lambda: client.index(
            index=index,
            id=ids[0],
            document=build_document(items[0]),
            version=1,
            version_type="external",
        ),
        409,
    )
    writer.publish(
        [items[0]], guard=guard, covered_by_newer=lambda _, doc: doc.get("deleted") is True
    )
    _check({doc["id"] for doc in reader.page(expressions["true"], scope).items} == {ids[1]})
