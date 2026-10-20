from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from appchi.search.bootstrap import _empty_index, exclusive_operation, validate
from appchi.search.bootstrap_probe import owned_mapping, target_identity, verify_mapping
from appchi.search.rebuild import RebuildBinding
from appchi.shared.errors import AppError


@pytest.fixture
def config(tmp_path):
    evidence = tmp_path / "clock.json"
    evidence.write_text(
        json.dumps(
            {
                "measured_at": datetime.now(UTC).isoformat(),
                "synchronized": True,
                "offset_seconds": 0,
            }
        )
    )
    return SimpleNamespace(
        environment="development",
        role="indexer",
        elasticsearch_url="https://es.example.invalid:9200",
        elasticsearch_index="appchi-test-alerts",
        clock_health_file=evidence,
        database_clock_health_file=evidence,
    )


@pytest.fixture
def binding():
    return RebuildBinding(
        alias="appchi-test-alerts",
        index_prefix="appchi-test-",
        capability_evidence="operator integration reference",
        atomic_alias_switch=True,
        exclusive_alias_writer=True,
        max_document_bytes=1048576,
        index_settings={"number_of_shards": 1, "number_of_replicas": 0},
    )


def test_valid_explicit_configuration(config, binding):
    validate(config, binding, "appchi-test-serving", str(uuid4()), "run-1", True)


@pytest.mark.parametrize(
    "changes",
    [
        {"environment": "production"},
        {"environment": "staging"},
        {"role": "ingestion-api"},
        {"elasticsearch_url": "http://localhost:9200"},
        {"elasticsearch_url": "https://es.example.invalid/proxy"},
        {"elasticsearch_url": "https://user:password@es.example.invalid"},
        {"elasticsearch_url": "https://es.example.invalid/?secret=x"},
        {"elasticsearch_index": "another-alias"},
        {"clock_health_file": None},
        {"database_clock_health_file": None},
    ],
)
def test_rejects_unsafe_configuration(config, binding, changes):
    for key, value in changes.items():
        setattr(config, key, value)
    with pytest.raises(AppError):
        validate(config, binding, "appchi-test-serving", str(uuid4()), "run-1", True)


@pytest.mark.parametrize(
    "physical,reference,stopped",
    [
        ("foreign-index", "ref", True),
        ("appchi-test-*", "ref", True),
        ("appchi-test-alerts", "ref", True),
        ("appchi-test-serving", " ", True),
        ("appchi-test-serving", "ref", False),
    ],
)
def test_requires_exact_names_reference_and_attestation(
    config, binding, physical, reference, stopped
):
    with pytest.raises(AppError):
        validate(config, binding, physical, str(uuid4()), reference, stopped)


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"number_of_shards": True, "number_of_replicas": 0},
        {"number_of_shards": 1, "number_of_replicas": -1},
        {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
    ],
)
def test_requires_supported_explicit_index_settings(config, binding, options):
    with pytest.raises(AppError):
        validate(
            config,
            binding.model_copy(update={"index_settings": options}),
            "appchi-test-serving",
            str(uuid4()),
            "ref",
            True,
        )


def test_stale_clock_is_not_an_integration_success_flag(config, binding):
    config.clock_health_file.write_text(
        json.dumps(
            {
                "measured_at": (datetime.now(UTC) - timedelta(seconds=61)).isoformat(),
                "synchronized": True,
                "offset_seconds": 0,
            }
        )
    )
    with pytest.raises(AppError, match="clock_untrusted"):
        validate(config, binding, "appchi-test-serving", str(uuid4()), "ref", True)


def test_mapping_checks_include_ownership_source_filters_and_extra_fields():
    client = MagicMock()
    expected = owned_mapping({"operation": "one"}, "serving-index")
    changed = []
    for key, value in [
        ("_meta", {}),
        ("dynamic", True),
        ("_source", {"enabled": True, "excludes": ["display"]}),
        ("runtime", {"hidden": {"type": "keyword"}}),
    ]:
        copy = deepcopy(expected)
        copy[key] = value
        changed.append(copy)
    for mapping in changed:
        client.indices.get_mapping.return_value = {"target": {"mappings": mapping}}
        with pytest.raises(AppError):
            verify_mapping(client, "target", expected)
    copy = deepcopy(expected)
    copy.pop("_source")
    client.indices.get_mapping.return_value = {"target": {"mappings": copy}}
    verify_mapping(client, "target", expected)


@pytest.mark.parametrize(
    "response",
    [
        {"timed_out": False, "hits": {"total": {"value": 0, "relation": "eq"}}},
        {
            "timed_out": True,
            "_shards": {"successful": 1, "total": 1},
            "hits": {"total": {"value": 0, "relation": "eq"}},
        },
        {
            "timed_out": False,
            "_shards": {"successful": 0, "total": 1},
            "hits": {"total": {"value": 0, "relation": "eq"}},
        },
        {
            "timed_out": False,
            "_shards": {"successful": 1, "total": 1},
            "hits": {"total": {"value": 1, "relation": "eq"}},
        },
        {
            "timed_out": False,
            "_shards": {"successful": 1, "total": 1},
            "hits": {"total": {"value": 0, "relation": "gte"}},
        },
    ],
)
def test_incomplete_or_nonempty_index_never_means_zero(response):
    client = MagicMock()
    client.indices.refresh.return_value = {"_shards": {"failed": 0}}
    client.search.return_value = response
    with pytest.raises(AppError):
        _empty_index(client, "target")
    client.indices.refresh.assert_called_once_with(index="target")


def test_unknown_target_identity_is_rejected():
    client = MagicMock()
    client.info.return_value = {"cluster_uuid": "_na_", "version": {"number": "9.0"}}
    with pytest.raises(AppError):
        target_identity(client)


def test_uncertain_advisory_acquisition_discards_session():
    conn = MagicMock()
    conn.execute.side_effect = [None, None, None, RuntimeError("connection lost")]
    with pytest.raises(RuntimeError), exclusive_operation(conn):
        pytest.fail("Must not enter without lock")
    conn.close.assert_called_once()


def test_busy_operation_does_not_unlock_another_session():
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = {"acquired": False}
    with pytest.raises(AppError), exclusive_operation(conn):
        pytest.fail("Must not enter while busy")
    assert not any("pg_advisory_unlock" in call.args[0] for call in conn.execute.call_args_list)
    conn.close.assert_not_called()
