"""Explicit initial search activation for isolated development diagnostics only."""

import argparse
from contextlib import contextmanager
from hashlib import sha256
import json
from pathlib import Path
import re
import time
from urllib.parse import urlsplit
from uuid import UUID

from psycopg.types.json import Jsonb

from keep.clock_health import require_clock_health
from keep.recovery.guard import runtime_guard
from keep.search.bootstrap_probe import owned_mapping, qualify, target_identity, verify_mapping
from keep.search.projection import assert_complete, canonical, index_mapping
from keep.search.rebuild import HOLD_LOCK, RebuildBinding, alias_target
from keep.shared.errors import AppError, unavailable

PENDING = "development_search_bootstrap_in_progress"
ACTIVE = "development_search_diagnostics_verified"
INTENT_KIND = "development-search-bootstrap-intent"


def validate(settings, binding, physical, epoch, reference, stopped):
    endpoint = urlsplit(settings.elasticsearch_url or "")
    if (
        settings.environment != "development"
        or settings.role != "indexer"
        or endpoint.scheme != "https"
        or not endpoint.hostname
        or endpoint.path not in {"", "/"}
        or endpoint.username
        or endpoint.password
        or endpoint.query
        or endpoint.fragment
        or not stopped
    ):
        raise unavailable("bootstrap_development_https_quiescence_required")
    if (
        settings.elasticsearch_index != binding.alias
        or physical == binding.alias
        or not physical.startswith(binding.index_prefix)
        or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,249}", physical)
        or not reference.strip()
        or len(reference) > 200
        or str(UUID(epoch)) != epoch
    ):
        raise unavailable("bootstrap_binding_invalid")
    options = binding.index_settings
    if (
        set(options) != {"number_of_shards", "number_of_replicas"}
        or type(options["number_of_shards"]) is not int
        or options["number_of_shards"] < 1
        or type(options["number_of_replicas"]) is not int
        or options["number_of_replicas"] < 0
    ):
        raise unavailable("bootstrap_explicit_shard_settings_required")
    if not settings.clock_health_file or not settings.database_clock_health_file:
        raise unavailable("clock_untrusted")
    require_clock_health(settings)


@contextmanager
def transaction(conn):
    with conn.transaction():
        conn.execute("SET LOCAL TIME ZONE 'UTC'")
        conn.execute("SET LOCAL statement_timeout='4000ms'")
        conn.execute("SET LOCAL lock_timeout='2000ms'")
        yield


@contextmanager
def exclusive_operation(conn):
    # The restore guard and this session lock span HTTP calls, but no SQL
    # transaction does. Uncertain acquisition/release closes the pooled session.
    acquired, known = False, False
    try:
        with transaction(conn):
            acquired = conn.execute(
                "SELECT pg_try_advisory_lock(%s) AS acquired", (HOLD_LOCK,)
            ).fetchone()["acquired"]
            known = True
            if not acquired:
                raise unavailable("bootstrap_operation_busy")
        yield
    finally:
        if acquired:
            try:
                with transaction(conn):
                    if not conn.execute(
                        "SELECT pg_advisory_unlock(%s) AS released", (HOLD_LOCK,)
                    ).fetchone()["released"]:
                        raise unavailable("bootstrap_lock_release_unproven")
            except Exception:
                conn.close()
                raise
        elif not known:
            conn.close()


def _initial(conn, deployment_epoch):
    restore = conn.execute("SELECT * FROM restore_control WHERE singleton").fetchone()
    if (
        not restore
        or restore["fenced"]
        or restore["run_id"] is not None
        or restore["deployment_epoch"] not in (None, deployment_epoch)
    ):
        raise unavailable("bootstrap_restore_not_supported")
    gate = conn.execute(
        "SELECT * FROM capability_gates WHERE capability='search' FOR UPDATE"
    ).fetchone()
    if not gate or (gate["ready"], gate["reason"]) not in {
        (False, "index_qualification_required"),
        (False, PENDING),
        (True, ACTIVE),
    }:
        raise unavailable("bootstrap_gate_not_initial")
    return gate


def _empty_locked(conn):
    # Canonical alert writes require these tables. NOWAIT rejects an undrained
    # writer instead of waiting while it may hold row/advisory locks we need.
    conn.execute(
        "LOCK TABLE tenants,tenant_work_state,tenant_view_state,alert_identities,projection_work IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    if conn.execute(
        "SELECT 1 WHERE EXISTS(SELECT 1 FROM alert_identities) OR EXISTS(SELECT 1 FROM projection_work)"
    ).fetchone():
        raise unavailable("bootstrap_explicit_rebuild_required")
    if conn.execute(
        "SELECT 1 FROM tenants t LEFT JOIN tenant_work_state w ON w.tenant_id=t.id "
        "LEFT JOIN tenant_view_state v ON v.tenant_id=t.id WHERE w.tenant_id IS NULL "
        "OR v.tenant_id IS NULL OR w.projection_seq<>0 OR w.change_order<>0 OR w.admission_order<>0 "
        "OR w.blocked OR w.last_clock>clock_timestamp() OR v.searchable_seq<>0 LIMIT 1"
    ).fetchone():
        raise unavailable("bootstrap_tenant_watermark_conflict")
    if conn.execute(
        "SELECT 1 FROM search_writer_leases WHERE expires_at>clock_timestamp() LIMIT 1"
    ).fetchone():
        raise unavailable("bootstrap_writer_not_drained")


def _control(conn, intent):
    control = conn.execute(
        "SELECT * FROM search_index_control WHERE singleton FOR UPDATE"
    ).fetchone()
    if control and (
        control["alias_name"] != intent["alias"]
        or control["physical_index"] != intent["physical_index"]
        or control["index_epoch"] != intent["index_epoch"]
        or control["binding_digest"] != intent["binding_sha256"]
        or control["active_rebuild_id"] is not None
    ):
        raise unavailable("bootstrap_binding_conflict")
    return control


def _view_epochs(conn, intent, control, gate):
    allowed = [intent["index_epoch"]] if gate["ready"] else ["uninitialized"]
    if control and not gate["ready"]:
        # New tenants inherit the reserved epoch through the existing trigger.
        allowed.append(intent["index_epoch"])
    if conn.execute(
        "SELECT 1 FROM tenant_view_state WHERE NOT (index_epoch=ANY(%s)) "
        "OR (%s AND verified_at IS NOT NULL) LIMIT 1",
        (allowed, not gate["ready"]),
    ).fetchone():
        raise unavailable("bootstrap_tenant_epoch_conflict")


def _reserve(conn, intent):
    with transaction(conn):
        gate = _initial(conn, intent["deployment_epoch"])
        _empty_locked(conn)
        control = _control(conn, intent)
        _view_epochs(conn, intent, control, gate)
        previous = conn.execute(
            "SELECT kind,evidence FROM audit_events WHERE id=%s", (intent["index_epoch"],)
        ).fetchone()
        if previous:
            if previous["kind"] != INTENT_KIND or previous["evidence"] != intent or not control:
                raise unavailable("bootstrap_intent_conflict")
            if control["writers_paused"] != (not gate["ready"]):
                raise unavailable("bootstrap_binding_conflict")
            if gate["ready"]:
                # Even an empty successful repeat must not expose an active gate
                # while repairing an absent index/alias or rerunning diagnostics.
                conn.execute("UPDATE search_index_control SET writers_paused=true WHERE singleton")
                conn.execute(
                    "UPDATE tenant_view_state SET verified_at=NULL,view_generation=view_generation+1"
                )
                conn.execute(
                    "UPDATE capability_gates SET ready=false,reason=%s WHERE capability='search'",
                    (PENDING,),
                )
        else:
            if control or gate["ready"] or gate["reason"] != "index_qualification_required":
                raise unavailable("bootstrap_intent_required")
            conn.execute(
                "INSERT INTO audit_events(id,actor_label,kind,evidence) VALUES(%s,'development-search-bootstrap',%s,%s)",
                (intent["index_epoch"], INTENT_KIND, Jsonb(intent)),
            )
            conn.execute(
                "INSERT INTO search_index_control(alias_name,physical_index,index_epoch,binding_digest,writers_paused) VALUES(%s,%s,%s,%s,true)",
                (
                    intent["alias"],
                    intent["physical_index"],
                    intent["index_epoch"],
                    intent["binding_sha256"],
                ),
            )
            conn.execute(
                "UPDATE capability_gates SET ready=false,reason=%s WHERE capability='search'",
                (PENDING,),
            )
        return gate["ready"]


def _empty_index(client, physical):
    # Refresh prevents invisible documents from being mistaken for an empty index.
    assert_complete(client.indices.refresh(index=physical))
    response = client.search(
        index=physical,
        query={"match_all": {}},
        size=0,
        track_total_hits=True,
        allow_partial_search_results=False,
    )
    assert_complete(response)
    shards = response.get("_shards", {})
    if (
        response.get("timed_out") is not False
        or type(shards.get("total")) is not int
        or shards["total"] < 1
        or shards.get("successful") != shards["total"]
        or shards.get("failed") != 0
        or response.get("hits", {}).get("total") != {"value": 0, "relation": "eq"}
    ):
        raise unavailable("bootstrap_index_not_empty")


def _ensure_index(client, binding, intent):
    physical, alias = intent["physical_index"], binding.alias
    expected = owned_mapping(intent, "serving-index")
    if client.indices.exists_alias(name=alias) and alias_target(client, alias) != physical:
        raise unavailable("bootstrap_alias_conflict")
    if not client.indices.exists(index=physical):
        if client.indices.exists(index=alias):
            raise unavailable("bootstrap_alias_conflict")
        client.indices.create(
            index=physical, mappings=expected, settings=binding.index_settings, aliases={alias: {}}
        )
    verify_mapping(client, physical, expected)
    settings = client.indices.get_settings(index=physical)[physical]["settings"]["index"]
    if any(settings.get(key) != str(value) for key, value in binding.index_settings.items()):
        raise unavailable("bootstrap_index_settings_conflict")
    aliases = client.indices.get_alias(index=physical)[physical].get("aliases", {})
    if set(aliases) - {alias}:
        raise unavailable("bootstrap_alias_conflict")
    _empty_index(client, physical)
    if not client.indices.exists_alias(name=alias):
        client.indices.update_aliases(actions=[{"add": {"index": physical, "alias": alias}}])
    if alias_target(client, alias) != physical:
        raise unavailable("bootstrap_alias_conflict")


def bootstrap(db, client, settings, binding, *, physical, epoch, reference, stopped):
    validate(settings, binding, physical, epoch, reference, stopped)
    intent = {
        "scope": "isolated-development-diagnostics",
        "production_qualified": False,
        "operation_reference": reference,
        "deployment_epoch": settings.deployment_epoch,
        "index_epoch": epoch,
        "alias": binding.alias,
        "physical_index": physical,
        "endpoint": settings.elasticsearch_url,
        "target": target_identity(client),
        "binding_sha256": binding.digest(),
        "capability_evidence_reference": binding.capability_evidence,
        "mapping_sha256": sha256(canonical(index_mapping())).hexdigest(),
        "quiescence_attested": True,
    }
    with (
        db.pool.connection() as conn,
        runtime_guard(conn, settings.deployment_epoch),
        exclusive_operation(conn),
    ):
        already_active = _reserve(conn, intent)
        _ensure_index(client, binding, intent)
        evidence = qualify(client, binding, intent)
        completed = time.monotonic()
        _ensure_index(client, binding, intent)
        if target_identity(client) != intent["target"]:
            raise unavailable("bootstrap_target_changed")
        require_clock_health(settings)
        with transaction(conn):
            gate = _initial(conn, settings.deployment_epoch)
            _empty_locked(conn)
            control = _control(conn, intent)
            if not control or control["writers_paused"] != (not gate["ready"]):
                raise unavailable("bootstrap_binding_changed")
            _view_epochs(conn, intent, control, gate)
            if time.monotonic() - completed > 60:
                raise unavailable("bootstrap_evidence_expired")
            # All current tenants are represented by the same globally empty ES
            # result. These updates and gate activation commit atomically.
            verified = conn.execute(
                "UPDATE tenant_view_state SET index_epoch=%s,verified_at=clock_timestamp(),view_generation=view_generation+1 WHERE searchable_seq=0",
                (epoch,),
            ).rowcount
            conn.execute(
                "UPDATE search_index_control SET writers_paused=false,updated_at=clock_timestamp() WHERE singleton"
            )
            conn.execute(
                "UPDATE capability_gates SET ready=true,reason=%s WHERE capability='search'",
                (ACTIVE,),
            )
            conn.execute(
                "INSERT INTO audit_events(actor_label,kind,evidence) VALUES('development-search-bootstrap','development-search-bootstrap-activated',%s)",
                (Jsonb({**intent, "live_probe": evidence, "verified_empty_tenants": verified}),),
            )
    return {
        "state": "already_active" if already_active else "active",
        "index_epoch": epoch,
        "verified_empty_tenants": verified,
        "production_qualified": False,
        "live_probe": evidence,
    }


def main():
    from elasticsearch import Elasticsearch
    from keep.config import Settings
    from keep.db import Database
    from keep.service_clients import elasticsearch_options

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--physical-index", required=True)
    parser.add_argument("--index-epoch", required=True)
    parser.add_argument("--operation-reference", required=True)
    parser.add_argument("--ingestion-and-writers-stopped", action="store_true", required=True)
    args = parser.parse_args()
    db = None
    try:
        raw = args.binding.read_bytes()
        if len(raw) > 65536:
            raise unavailable("bootstrap_binding_invalid")
        binding, settings = RebuildBinding.model_validate_json(raw), Settings()
        validate(
            settings,
            binding,
            args.physical_index,
            args.index_epoch,
            args.operation_reference,
            args.ingestion_and_writers_stopped,
        )
        db = Database(
            settings.database_url.get_secret_value(), 1, deployment_epoch=settings.deployment_epoch
        )
        with Elasticsearch(settings.elasticsearch_url, **elasticsearch_options(settings)) as client:
            result = bootstrap(
                db,
                client,
                settings,
                binding,
                physical=args.physical_index,
                epoch=args.index_epoch,
                reference=args.operation_reference,
                stopped=args.ingestion_and_writers_stopped,
            )
        print(json.dumps(result))
    except Exception as error:
        # Do not leak DSNs, credentials, server error bodies or certificate paths.
        print(
            json.dumps(
                {
                    "error": error.code
                    if isinstance(error, AppError)
                    else "development_search_bootstrap_failed"
                }
            )
        )
        raise SystemExit(1) from None
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    main()
