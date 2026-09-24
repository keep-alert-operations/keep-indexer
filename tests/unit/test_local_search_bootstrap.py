from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

from pydantic import SecretStr
import pytest

from appchi.search.local_bootstrap import (
    ALIAS,
    PHYSICAL,
    REASON,
    bootstrap,
    check_initial_gate,
    compatibility_evidence,
    validate_local,
    verify_empty_tenants,
    verify_mapping,
)
from appchi.search.projection import index_mapping
from appchi.shared.errors import AppError


def settings(**changes):
    values = dict(
        environment="development",
        elasticsearch_url="http://127.0.0.1:59200",
        elasticsearch_index=ALIAS,
        database_url=SecretStr("postgresql://worker@127.0.0.1:55432/appchi_local"),
    )
    values.update(changes)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    "changes",
    [
        {"environment": "production"},
        {"elasticsearch_url": "http://es.company:9200"},
        {"elasticsearch_url": "http://localhost:59200/proxy"},
        {"elasticsearch_index": "shared-alerts"},
        {"database_url": SecretStr("postgresql://worker@localhost/shared")},
        {"database_url": SecretStr("dbname=appchi_local host=localhost hostaddr=192.0.2.1")},
        {"database_url": SecretStr("dbname=appchi_local host=localhost service=shared")},
    ],
)
def test_rejects_nonisolated_settings(changes):
    with pytest.raises(AppError) as error:
        validate_local(settings(**changes))
    assert error.value.code == "local_search_environment_required"


def test_junit_requires_both_real_suites_without_skips_or_failures(tmp_path):
    path = tmp_path / "evidence.xml"
    valid = (
        '<testsuites><testsuite><testcase classname="tests.compatibility.test_search_elasticsearch"/>'
        '<testcase classname="tests.compatibility.test_count_elasticsearch"/></testsuite></testsuites>'
    )
    path.write_text(valid)
    assert compatibility_evidence(path)["tests"] == 2
    for changed in (
        valid.replace("test_count_elasticsearch", "test_other"),
        valid.replace("/>", "><skipped/></testcase>", 1),
        valid.replace("/>", "><failure/></testcase>", 1),
        "<invalid",
    ):
        path.write_text(changed)
        with pytest.raises(AppError) as error:
            compatibility_evidence(path)
        assert error.value.code == "local_search_evidence_required"


@pytest.mark.parametrize(
    "restore,gate,code",
    [
        (
            {"fenced": True, "run_id": None},
            {"ready": False, "reason": "index_qualification_required"},
            "local_search_restore_not_supported",
        ),
        (
            {"fenced": False, "run_id": "past-restore"},
            {"ready": False, "reason": "index_qualification_required"},
            "local_search_restore_not_supported",
        ),
        (
            {"fenced": False, "run_id": None},
            {"ready": False, "reason": "recovery_unreconciled"},
            "local_search_gate_not_initial",
        ),
        (
            {"fenced": False, "run_id": None},
            {"ready": False, "reason": REASON},
            "local_search_gate_not_initial",
        ),
    ],
)
def test_never_opens_noninitial_gate(restore, gate, code):
    conn = MagicMock()
    conn.execute.return_value.fetchone.side_effect = [restore, gate]
    with pytest.raises(AppError) as error:
        check_initial_gate(conn)
    assert error.value.code == code
    assert not any(
        "UPDATE capability_gates" in call.args[0] for call in conn.execute.call_args_list
    )


def test_restore_rejection_precedes_all_elasticsearch_mutation():
    conn, client = MagicMock(), MagicMock()
    conn.execute.return_value.fetchone.side_effect = [
        {"fenced": True, "run_id": "restore"},
        {"ready": False, "reason": "index_qualification_required"},
    ]
    db = MagicMock()
    db.transaction.return_value.__enter__.return_value = conn
    with pytest.raises(AppError):
        bootstrap(db, client, settings(), {})
    assert client.mock_calls == []


@pytest.mark.parametrize("change", ["dynamic", "properties", "_meta", "_source"])
def test_mapping_mismatch_is_rejected(change):
    mapping = deepcopy(index_mapping())
    mapping[change] = False if change == "dynamic" else {"enabled": False}
    client = MagicMock()
    client.indices.get_mapping.return_value = {PHYSICAL: {"mappings": mapping}}
    with pytest.raises(AppError) as error:
        verify_mapping(client, PHYSICAL)
    assert error.value.code == "search_mapping_mismatch"


def test_valid_mapping_is_accepted():
    client = MagicMock()
    client.indices.get_mapping.return_value = {PHYSICAL: {"mappings": index_mapping()}}
    verify_mapping(client, PHYSICAL)


def test_empty_proof_rechecks_projection_after_external_query(monkeypatch):
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [{"tenant_id": "tenant"}]
    conn.execute.return_value.fetchone.return_value = {
        "index_epoch": "epoch",
        "writers_paused": False,
        "active_rebuild_id": None,
        "physical_index": PHYSICAL,
    }

    @contextmanager
    def transaction(*args):
        yield conn

    db = SimpleNamespace(transaction=transaction)
    client = MagicMock()
    client.search.return_value = {"hits": {"total": {"value": 0, "relation": "eq"}}}
    monkeypatch.setattr(
        "appchi.search.local_bootstrap.coordinator",
        lambda *args: ({"blocked": False, "projection_seq": 1}, None),
    )
    monkeypatch.setattr("appchi.search.local_bootstrap.check_initial_gate", lambda *args: None)
    assert verify_empty_tenants(db, client, "epoch") == 0
    assert client.search.call_args.kwargs["allow_partial_search_results"] is False
    assert not any(
        "UPDATE tenant_view_state" in call.args[0] for call in conn.execute.call_args_list
    )
