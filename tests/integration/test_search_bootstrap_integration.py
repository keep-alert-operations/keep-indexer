"""Opt-in, disposable databases and namespaced ES indices; never reuse a product DB."""

from datetime import UTC, datetime
import base64
import json
import os
import subprocess
import sys
from types import SimpleNamespace
from uuid import uuid4

from elasticsearch import Elasticsearch
import psycopg
from psycopg.conninfo import make_conninfo
from psycopg import sql
import pytest

from appchi.db import Database
from appchi.recovery.guard import RESTORE_LOCK
from appchi.search import bootstrap as command
from appchi.search.bootstrap_probe import owned_mapping
from appchi.search.projection import index_mapping
from appchi.search.rebuild import HOLD_LOCK, RebuildBinding, alias_target, start_rebuild
from appchi.shared.errors import AppError

pytestmark = pytest.mark.integration


@pytest.fixture
def system(tmp_path):
    admin_dsn = os.environ.get("APPCHI_BOOTSTRAP_TEST_POSTGRES_ADMIN_DSN")
    endpoint = os.environ.get("APPCHI_TEST_ELASTICSEARCH_URL")
    if not admin_dsn or not endpoint:
        pytest.skip("Requires explicit disposable-database admin DSN and isolated Elasticsearch")
    admin_dsn = make_conninfo(admin_dsn, connect_timeout=5)
    from appchi.migrate import migrate, migration_directory

    name = "bootstrap_test_" + uuid4().hex
    prefix = "appchi-bootstrap-test-" + uuid4().hex + "-"
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = make_conninfo(admin_dsn, dbname=name)
    db, client = None, None
    try:
        migrate(dsn, migration_directory())
        # Run the operation with actual runtime privileges, not database owner privileges.
        db = Database(make_conninfo(dsn, options="-c role=appchi_worker"), 2)
        # TLS connection setup on a constrained workstation can exceed the
        # runtime pool's 3s checkout budget. Warm it without changing that budget.
        with db.pool.connection(timeout=15) as conn:
            assert conn.execute("SELECT current_user").fetchone()["current_user"] == "appchi_worker"
            conn.commit()
        ca = os.environ.get("APPCHI_TEST_ELASTICSEARCH_CA")
        api_key = os.environ.get("APPCHI_TEST_ELASTICSEARCH_API_KEY")
        client = Elasticsearch(
            endpoint,
            request_timeout=10,
            max_retries=0,
            **({"ca_certs": ca} if ca else {}),
            **({"api_key": api_key} if api_key else {}),
        )
        clock = tmp_path / "clock.json"
        settings = SimpleNamespace(
            environment="development",
            role="indexer",
            deployment_epoch="bootstrap-test",
            elasticsearch_url=endpoint,
            elasticsearch_index=prefix + "alerts",
            clock_health_file=clock,
            database_clock_health_file=clock,
        )
        binding = RebuildBinding(
            alias=settings.elasticsearch_index,
            index_prefix=prefix,
            capability_evidence="isolated integration test fixture",
            atomic_alias_switch=True,
            exclusive_alias_writer=True,
            max_document_bytes=1048576,
            index_settings={"number_of_shards": 1, "number_of_replicas": 0},
        )
        args = dict(
            physical=prefix + "serving",
            epoch=str(uuid4()),
            reference="isolated-integration",
            stopped=True,
        )

        def run(**changes):
            clock.write_text(
                json.dumps(
                    {
                        "measured_at": datetime.now(UTC).isoformat(),
                        "synchronized": True,
                        "offset_seconds": 0,
                    }
                )
            )
            return command.bootstrap(db, client, settings, binding, **(args | changes))

        yield SimpleNamespace(
            db=db, client=client, dsn=dsn, settings=settings, binding=binding, args=args, run=run
        )
    finally:
        if db:
            db.close()
        if client:
            # This random namespace belongs exclusively to this fixture.
            for index in client.indices.get(index=prefix + "*", allow_no_indices=True):
                client.indices.delete(index=index)
            client.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def gate(system):
    with system.db.transaction() as conn:
        return conn.execute(
            "SELECT ready,reason FROM capability_gates WHERE capability='search'"
        ).fetchone()


def test_fresh_activation_live_probe_and_exact_repeat(system):
    result = system.run()
    assert result["state"] == "active" and result["production_qualified"] is False
    assert result["live_probe"]["target"]["cluster_uuid"]
    assert "exact-count-batch" in result["live_probe"]["checks"]
    assert gate(system) == {"ready": True, "reason": command.ACTIVE}
    assert alias_target(system.client, system.binding.alias) == system.args["physical"]
    with system.db.transaction() as conn:
        rows = conn.execute("SELECT * FROM tenant_view_state").fetchall()
        assert rows and all(
            row["index_epoch"] == system.args["epoch"]
            and row["verified_at"]
            and row["searchable_seq"] == 0
            for row in rows
        )
        evidence = conn.execute(
            "SELECT evidence FROM audit_events WHERE kind='development-search-bootstrap-activated'"
        ).fetchone()["evidence"]
        assert evidence["scope"] == "isolated-development-diagnostics"
        assert evidence["live_probe"] == result["live_probe"]
    assert system.run()["state"] == "already_active"
    assert start_rebuild(system.db, system.binding)


def test_standalone_cli_activates_with_real_settings(system, tmp_path):
    binding_path = tmp_path / "binding.json"
    binding_path.write_text(system.binding.model_dump_json())
    system.settings.clock_health_file.write_text(
        json.dumps(
            {
                "measured_at": datetime.now(UTC).isoformat(),
                "synchronized": True,
                "offset_seconds": 0,
            }
        )
    )
    environment = {key: value for key, value in os.environ.items() if not key.startswith("APPCHI_")}
    environment.update(
        {
            "APPCHI_ENVIRONMENT": "development",
            "APPCHI_ROLE": "indexer",
            "APPCHI_DEPLOYMENT_EPOCH": system.settings.deployment_epoch,
            "APPCHI_DATABASE_URL": make_conninfo(system.dsn, options="-c role=appchi_worker"),
            "APPCHI_WRAPPING_KEY": base64.b64encode(b"test" * 8).decode(),
            "APPCHI_ELASTICSEARCH_URL": system.settings.elasticsearch_url,
            "APPCHI_ELASTICSEARCH_INDEX": system.binding.alias,
            "APPCHI_CLOCK_HEALTH_FILE": str(system.settings.clock_health_file.resolve()),
            "APPCHI_DATABASE_CLOCK_HEALTH_FILE": str(
                system.settings.database_clock_health_file.resolve()
            ),
        }
    )
    for suffix in ("CA", "API_KEY"):
        if value := os.environ.get("APPCHI_TEST_ELASTICSEARCH_" + suffix):
            environment["APPCHI_ELASTICSEARCH_" + ("CA_FILE" if suffix == "CA" else suffix)] = value
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "appchi.search.bootstrap",
            "--binding",
            str(binding_path),
            "--physical-index",
            system.args["physical"],
            "--index-epoch",
            system.args["epoch"],
            "--operation-reference",
            system.args["reference"],
            "--ingestion-and-writers-stopped",
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert completed.returncode == 0, completed.stdout
    assert json.loads(completed.stdout)["state"] == "active"
    assert gate(system)["ready"] is True


def test_creation_failure_resumes_matching_reservation(system, monkeypatch):
    original = system.client.indices.create

    def fail(**kwargs):
        raise RuntimeError("simulated network failure before index creation")

    monkeypatch.setattr(system.client.indices, "create", fail)
    with pytest.raises(RuntimeError):
        system.run()
    assert gate(system) == {"ready": False, "reason": command.PENDING}
    with pytest.raises(AppError, match="bootstrap_intent_conflict"):
        system.run(reference="different-operation")
    monkeypatch.setattr(system.client.indices, "create", original)
    assert system.run()["state"] == "active"


def test_failed_repeat_closes_gate_until_verified_again(system, monkeypatch):
    assert system.run()["state"] == "active"
    real = command.qualify

    def fail(*args):
        raise RuntimeError("fresh repeat diagnostics failed")

    monkeypatch.setattr(command, "qualify", fail)
    with pytest.raises(RuntimeError):
        system.run()
    assert gate(system) == {"ready": False, "reason": command.PENDING}
    with system.db.transaction() as conn:
        assert (
            conn.execute("SELECT writers_paused FROM search_index_control").fetchone()[
                "writers_paused"
            ]
            is True
        )
        assert (
            conn.execute(
                "SELECT count(*) AS count FROM tenant_view_state WHERE verified_at IS NOT NULL"
            ).fetchone()["count"]
            == 0
        )
    monkeypatch.setattr(command, "qualify", real)
    assert system.run()["state"] == "active"


def test_probe_failure_leaves_owned_index_for_safe_retry(system, monkeypatch):
    real = command.qualify

    def fail(*args):
        raise RuntimeError("simulated crash after target creation")

    monkeypatch.setattr(command, "qualify", fail)
    with pytest.raises(RuntimeError):
        system.run()
    assert gate(system)["ready"] is False
    assert system.client.indices.exists(index=system.args["physical"])
    with pytest.raises(AppError, match="bootstrap_binding_conflict"):
        system.run(epoch=str(uuid4()))
    # Reconcile the partial external outcome: owned target exists, alias missing.
    system.client.indices.delete_alias(index=system.args["physical"], name=system.binding.alias)
    monkeypatch.setattr(command, "qualify", real)
    assert system.run()["state"] == "active"


@pytest.mark.parametrize(
    "mutation,code",
    [
        ("UPDATE restore_control SET fenced=true", "restore_reconciliation_required"),
        (
            "UPDATE restore_control SET deployment_epoch='stale-deployment'",
            "deployment_epoch_fenced",
        ),
        ("UPDATE tenant_work_state SET projection_seq=1", "bootstrap_tenant_watermark_conflict"),
        ("UPDATE tenant_work_state SET blocked=true", "bootstrap_tenant_watermark_conflict"),
        (
            "UPDATE tenant_work_state SET last_clock=clock_timestamp()+interval '1 hour'",
            "bootstrap_tenant_watermark_conflict",
        ),
        (
            "UPDATE tenant_view_state SET index_epoch='unexpected'",
            "bootstrap_tenant_epoch_conflict",
        ),
        (
            "INSERT INTO alert_identities(id,tenant_id) SELECT gen_random_uuid(),id FROM tenants",
            "bootstrap_explicit_rebuild_required",
        ),
        (
            "INSERT INTO search_writer_leases(id,index_epoch,expires_at) VALUES(gen_random_uuid(),'old',clock_timestamp()+interval '1 hour')",
            "bootstrap_writer_not_drained",
        ),
        (
            "UPDATE capability_gates SET reason='restore_unreconciled' WHERE capability='search'",
            "bootstrap_gate_not_initial",
        ),
    ],
)
def test_rejects_noninitial_database_before_es_mutation(system, mutation, code):
    with psycopg.connect(system.dsn) as conn:
        conn.execute(mutation)
    with pytest.raises(AppError, match=code):
        system.run()
    assert not system.client.indices.exists(index=system.args["physical"])
    assert gate(system)["ready"] is False


def test_completed_restore_is_not_initial_even_with_gate_reset(system):
    with psycopg.connect(system.dsn) as conn:
        # Real restore row shape, with coordinator-only updates through the test admin.
        identity = conn.execute(
            "INSERT INTO restore_runs(id,previous_epoch,deployment_epoch,operation_reference,state) "
            "VALUES(gen_random_uuid(),'old',gen_random_uuid(),'test-backup','collected') RETURNING id"
        ).fetchone()[0]
        conn.execute("UPDATE restore_control SET run_id=%s", (identity,))
    with pytest.raises(AppError, match="bootstrap_restore_not_supported"):
        system.run()
    assert not system.client.indices.exists(index=system.args["physical"])


def test_foreign_index_is_never_adopted(system):
    system.client.indices.create(index=system.args["physical"], mappings=index_mapping())
    with pytest.raises(AppError, match="bootstrap_mapping_mismatch"):
        system.run()
    assert gate(system)["ready"] is False


def test_populated_owned_index_is_rejected_even_before_refresh(system, monkeypatch):
    def interrupt(*args):
        raise RuntimeError("pause before probe")

    monkeypatch.setattr(command, "qualify", interrupt)
    with pytest.raises(RuntimeError):
        system.run()
    system.client.index(index=system.args["physical"], id="unexpected", document={"deleted": True})
    with pytest.raises(AppError, match="bootstrap_index_not_empty"):
        system.run()
    assert gate(system)["ready"] is False


def test_final_check_detects_source_write_during_external_work(system, monkeypatch):
    def race(*args):
        with psycopg.connect(system.dsn) as conn:
            conn.execute(
                "INSERT INTO alert_identities(id,tenant_id) SELECT gen_random_uuid(),id FROM tenants"
            )
        return {"test": "injected write, not a capability result"}

    monkeypatch.setattr(command, "qualify", race)
    with pytest.raises(AppError, match="bootstrap_explicit_rebuild_required"):
        system.run()
    assert gate(system)["ready"] is False


def test_final_lock_rejects_inflight_writer(system, monkeypatch):
    with psycopg.connect(system.dsn) as writer:

        def race(*args):
            writer.execute("UPDATE tenant_work_state SET last_clock=clock_timestamp()")
            return {"test": "inflight write"}

        monkeypatch.setattr(command, "qualify", race)
        with pytest.raises(psycopg.errors.LockNotAvailable):
            system.run()
        writer.rollback()
    assert gate(system)["ready"] is False


def test_restore_cannot_start_during_live_probe_and_bootstrap_is_serialized(system, monkeypatch):
    def check(*args):
        with psycopg.connect(system.dsn) as other:
            assert (
                other.execute("SELECT pg_try_advisory_xact_lock(%s)", (RESTORE_LOCK,)).fetchone()[0]
                is False
            )
            assert (
                other.execute("SELECT pg_try_advisory_xact_lock(%s)", (HOLD_LOCK,)).fetchone()[0]
                is False
            )
        raise RuntimeError("stop after concurrency assertions")

    monkeypatch.setattr(command, "qualify", check)
    with pytest.raises(RuntimeError):
        system.run()
    assert gate(system)["ready"] is False


def test_probe_crash_residue_has_exact_ownership(system, monkeypatch):
    real = command.qualify

    def leave_residue(client, binding, intent):
        name = binding.index_prefix + "bootstrap-probe-" + intent["index_epoch"] + "-a"
        client.indices.create(
            index=name,
            mappings=owned_mapping(intent, "disposable-probe"),
            settings=binding.index_settings,
        )
        raise RuntimeError("simulated process death in probe")

    monkeypatch.setattr(command, "qualify", leave_residue)
    with pytest.raises(RuntimeError):
        system.run()
    monkeypatch.setattr(command, "qualify", real)
    assert system.run()["state"] == "active"
