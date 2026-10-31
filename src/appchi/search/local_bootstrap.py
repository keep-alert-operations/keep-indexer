"""Explicit isolated development search setup, never a restore readiness bypass."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
import xml.etree.ElementTree as ET

from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb

from appchi.recovery.guard import RESTORE_LOCK
from appchi.search.projection import assert_complete, index_mapping
from appchi.search.rebuild import RebuildBinding, alias_target, bind_serving_index
from appchi.shared.errors import AppError, unavailable
from appchi.shared.transactions import coordinator

ALIAS = "appchi-local-alerts"
PHYSICAL = ALIAS + "-v1"
REASON = "local_search_compatibility_verified"
LOOPBACK = {"localhost", "127.0.0.1", "::1"}


def validate_local(settings):
    endpoint = urlsplit(settings.elasticsearch_url or "")
    database = conninfo_to_dict(settings.database_url.get_secret_value())
    if (
        settings.environment != "development"
        or endpoint.hostname not in LOOPBACK
        or endpoint.scheme not in {"http", "https"}
        or endpoint.path not in {"", "/"}
        or endpoint.username
        or endpoint.password
        or endpoint.query
        or endpoint.fragment
        or settings.elasticsearch_index != ALIAS
        or database.get("dbname") != "appchi_local"
        or database.get("host") not in LOOPBACK
        or database.get("hostaddr", database.get("host")) not in LOOPBACK
        or database.get("service")
    ):
        raise unavailable("local_search_environment_required")


def compatibility_evidence(path):
    raw = path.read_bytes()
    if len(raw) > 2 * 1024 * 1024:
        raise unavailable("local_search_evidence_required")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        raise unavailable("local_search_evidence_required") from None
    cases = list(root.iter("testcase"))
    required = {"test_search_elasticsearch", "test_count_elasticsearch"}
    observed = {case.get("classname", "").split(".")[-1] for case in cases}
    if (
        not required.issubset(observed)
        or any(
            case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")
        )
        or any(True for _ in root.iter("failure"))
        or any(True for _ in root.iter("error"))
    ):
        raise unavailable("local_search_evidence_required")
    return {
        "junit_path": str(path.resolve()),
        "sha256": sha256(raw).hexdigest(),
        "tests": len(cases),
    }


def binding():
    return RebuildBinding(
        alias=ALIAS,
        index_prefix=ALIAS + "-",
        capability_evidence="Isolated local ES compatibility; exact JUnit digest in local-search-bootstrap audit",
        atomic_alias_switch=True,
        exclusive_alias_writer=True,
        max_document_bytes=1048576,
        index_settings={"number_of_shards": 1, "number_of_replicas": 0},
    )


def check_initial_gate(conn):
    # Workers may read restore control but cannot obtain a row lock requiring
    # UPDATE privilege. The restore coordinator owns this advisory lock exclusively.
    conn.execute("SELECT pg_advisory_xact_lock_shared(%s)", (RESTORE_LOCK,))
    restore = conn.execute("SELECT * FROM restore_control WHERE singleton").fetchone()
    gate = conn.execute(
        "SELECT * FROM capability_gates WHERE capability='search' FOR UPDATE"
    ).fetchone()
    if not restore or restore["fenced"] or restore["run_id"] is not None:
        raise unavailable("local_search_restore_not_supported")
    if not gate or (gate["ready"], gate["reason"]) not in {
        (False, "index_qualification_required"),
        (True, REASON),
    }:
        raise unavailable("local_search_gate_not_initial")
    return gate


def check_empty_database(conn):
    if conn.execute(
        "SELECT 1 WHERE EXISTS(SELECT 1 FROM alert_identities) OR EXISTS(SELECT 1 FROM projection_work)"
    ).fetchone():
        raise unavailable("local_search_explicit_rebuild_required")


def verify_mapping(client, physical, meta=None):
    observed = client.indices.get_mapping(index=physical)[physical]["mappings"]
    expected = index_mapping()
    if (
        observed.get("dynamic") != "strict"
        or observed.get("_meta") != (meta or expected["_meta"])
        or observed.get("properties") != expected["properties"]
        or observed.get("_source", {}).get("enabled", True) is not True
    ):
        raise unavailable("search_mapping_mismatch")


def serving_index(conn, current, selected):
    """The recorded serving index and its expected mapping metadata: the initial
    index, or a target the rebuild CLI completed under this same local binding.

    A mapping change that rewrites every document (REV-54) needs a rebuild, which
    records its job and epoch in the new index's metadata.
    """
    meta = index_mapping()["_meta"]
    if current is None or current["physical_index"] == PHYSICAL:
        return PHYSICAL, meta
    job = conn.execute(
        "SELECT id,target_epoch FROM search_rebuild_jobs WHERE target_index=%s AND state='complete'",
        (current["physical_index"],),
    ).fetchone()
    if (
        not current["physical_index"].startswith(selected.index_prefix)
        or not job
        or job["target_epoch"] != current["index_epoch"]
    ):
        raise unavailable("local_search_binding_conflict")
    meta.update(rebuild_id=str(job["id"]), index_epoch=job["target_epoch"])
    return current["physical_index"], meta


def bootstrap(db, client, settings, evidence):
    validate_local(settings)
    selected = binding()
    with db.transaction() as conn:
        gate = check_initial_gate(conn)
        current = conn.execute("SELECT * FROM search_index_control WHERE singleton").fetchone()
        if not current:
            check_empty_database(conn)
        elif (
            current["writers_paused"]
            or current["active_rebuild_id"]
            or current["alias_name"] != ALIAS
            or current["binding_digest"] != selected.digest()
        ):
            raise unavailable("local_search_binding_conflict")
        physical, meta = serving_index(conn, current, selected)
        epoch = current["index_epoch"] if current else str(uuid4())
        already_active = gate["ready"]
    if not client.indices.exists_alias(name=ALIAS):
        if current or client.indices.exists(index=PHYSICAL) or client.indices.exists(index=ALIAS):
            raise unavailable("local_search_unexpected_index")
        client.indices.create(
            index=PHYSICAL,
            mappings=index_mapping(),
            settings=selected.index_settings,
            aliases={ALIAS: {}},
        )
    if alias_target(client, ALIAS) != physical:
        raise unavailable("local_search_binding_conflict")
    verify_mapping(client, physical, meta)
    if not current:
        # No documents can be imported implicitly into a fresh local authority.
        response = client.search(
            index=PHYSICAL,
            query={"match_all": {}},
            size=0,
            track_total_hits=True,
            allow_partial_search_results=False,
        )
        assert_complete(response)
        if response["hits"]["total"] != {"value": 0, "relation": "eq"}:
            raise unavailable("local_search_unexpected_documents")
        bind_serving_index(db, client, selected, epoch)
    verified = verify_empty_tenants(db, client, epoch, physical)
    with db.transaction() as conn:
        gate = check_initial_gate(conn)
        control = conn.execute(
            "SELECT * FROM search_index_control WHERE singleton FOR SHARE"
        ).fetchone()
        if (
            not control
            or control["index_epoch"] != epoch
            or control["writers_paused"]
            or control["active_rebuild_id"]
            or control["binding_digest"] != selected.digest()
        ):
            raise unavailable("local_search_binding_changed")
        if not gate["ready"]:
            check_empty_database(conn)
            conn.execute(
                "UPDATE capability_gates SET ready=true,reason=%s WHERE capability='search'",
                (REASON,),
            )
            conn.execute(
                "INSERT INTO audit_events(tenant_id,actor_id,actor_label,kind,evidence) VALUES(NULL,NULL,'local-development-bootstrap','local-search-bootstrap',%s)",
                (
                    Jsonb(
                        {
                            **evidence,
                            "index_epoch": epoch,
                            "alias": ALIAS,
                            "physical_index": physical,
                            "scope": "isolated-local-development",
                            "production_qualified": False,
                        }
                    ),
                ),
            )
    return {
        "state": "already_active" if already_active else "active",
        "verified_empty_tenants": verified,
        "production_qualified": False,
    }


def verify_empty_tenants(db, client, epoch, physical=PHYSICAL):
    with db.transaction() as conn:
        tenants = conn.execute(
            "SELECT v.tenant_id FROM tenant_view_state v JOIN tenant_work_state w USING(tenant_id) "
            "WHERE v.verified_at IS NULL AND v.searchable_seq=0 AND w.projection_seq=0 AND v.index_epoch=%s",
            (epoch,),
        ).fetchall()
    verified = 0
    for row in tenants:
        tenant = row["tenant_id"]
        response = client.search(
            index=physical,
            query={"term": {"tenant_id": str(tenant)}},
            size=0,
            track_total_hits=True,
            allow_partial_search_results=False,
        )
        assert_complete(response)
        if response["hits"]["total"] != {"value": 0, "relation": "eq"}:
            raise unavailable("local_search_unexpected_documents")
        with db.transaction(tenant) as conn:
            state, now = coordinator(conn, tenant)
            check_initial_gate(conn)
            control = conn.execute(
                "SELECT * FROM search_index_control WHERE singleton FOR SHARE"
            ).fetchone()
            if (
                not control
                or control["index_epoch"] != epoch
                or control["writers_paused"]
                or control["active_rebuild_id"]
                or control["physical_index"] != physical
            ):
                raise unavailable("local_search_binding_changed")
            if state["blocked"] or state["projection_seq"] != 0:
                continue
            if conn.execute(
                "SELECT 1 FROM projection_work WHERE tenant_id=%s LIMIT 1", (tenant,)
            ).fetchone():
                continue
            verified += conn.execute(
                "UPDATE tenant_view_state SET verified_at=%s,view_generation=view_generation+1 "
                "WHERE tenant_id=%s AND index_epoch=%s AND searchable_seq=0 AND verified_at IS NULL",
                (now, tenant, epoch),
            ).rowcount
    return verified


def main():
    from elasticsearch import Elasticsearch
    from appchi.config import Settings
    from appchi.db import Database
    from appchi.service_clients import elasticsearch_options

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    database = None
    try:
        settings = Settings()
        validate_local(settings)
        evidence = compatibility_evidence(args.evidence)
        database = Database(
            settings.database_url.get_secret_value(), 1, deployment_epoch=settings.deployment_epoch
        )
        with Elasticsearch(settings.elasticsearch_url, **elasticsearch_options(settings)) as client:
            print(json.dumps(bootstrap(database, client, settings, evidence)))
    except Exception as error:
        print(
            json.dumps(
                {
                    "error": error.code
                    if isinstance(error, AppError)
                    else "local_search_bootstrap_failed"
                }
            )
        )
        raise SystemExit(1) from None
    finally:
        if database:
            database.close()


if __name__ == "__main__":
    main()
