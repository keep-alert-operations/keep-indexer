"""The steps of one alert change inside the indexer, as log records."""

from datetime import UTC, datetime
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from keep.observability.testing import capture_records
from keep.search import consumer
from keep.shared.errors import AppError

TENANT = uuid4()
SNAPSHOT = {"application": "billing", "component": "database", "severity": "critical"}


def hint(alert, offset):
    body = json.dumps(
        {
            "event_id": f"change-{offset}",
            "tenant_id": str(TENANT),
            "alert_id": str(alert),
            "cause": "source",
            "projection_seq": str(offset),
            "recorded_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
    ).encode()
    return SimpleNamespace(
        value=lambda: body, key=lambda: TENANT.bytes, partition=lambda: 1, offset=lambda: offset
    )


@pytest.fixture
def records():
    return capture_records("indexer", "indexer")


@pytest.fixture
def searchable(monkeypatch):
    """A projection service that makes the given alerts visible, as the real one reports."""

    def use(visible):
        service = SimpleNamespace(projected={})

        def project_many(requests):
            service.projected = {
                key: SimpleNamespace(snapshot=SNAPSHOT, version=6)
                for key in requests
                if key[1] in visible
            }
            return set(service.projected)

        service.project_many = project_many
        service.project = lambda tenant, alerts: {
            alert for _, alert in project_many([(str(tenant), str(a)) for a in alerts])
        }
        monkeypatch.setattr(
            consumer,
            "ChangedEnvelope",
            SimpleNamespace(model_validate=lambda data: SimpleNamespace(**data)),
        )
        monkeypatch.setattr(consumer, "service_for", lambda *args: service)

    return use


def actions(records):
    return [(r["keep"]["alert"]["id"], r["event"]["sequence"], r["event"]["action"]) for r in records]


def test_each_change_is_recorded_as_read_and_then_as_searchable(records, searchable):
    """Requirements: REV-74; observability 3."""
    a, b = str(uuid4()), str(uuid4())
    searchable({a, b})
    assert consumer.index_batch(None, None, None, [hint(a, 10), hint(b, 11)]) == 2
    assert actions(records()) == [
        (a, 1, "alert_change_consumed"),
        (b, 1, "alert_change_consumed"),
        (a, 2, "alert_indexed"),
        (b, 2, "alert_indexed"),
    ]
    indexed = records()[2]
    assert indexed["keep"]["changed_event_id"] == "change-10"
    assert indexed["keep"]["kafka"] == {"consumer": "indexer", "partition": 1, "offset": 10}
    assert indexed["keep"]["projection_seq"] == 10 and indexed["keep"]["revision"] == 6
    assert indexed["keep"]["alert"]["application"] == "billing"
    assert indexed["keep"]["delay_seconds"] >= 0
    assert indexed["service"]["name"] == "indexer"


def test_a_change_the_bulk_did_not_reach_is_recorded_as_deferred(records, searchable):
    """Requirements: REV-74; observability 3."""
    a, b, c = str(uuid4()), str(uuid4()), str(uuid4())
    searchable({a, c})
    groups = [[hint(a, 10), hint(b, 11), hint(c, 12)]]
    assert consumer.index_groups(None, None, None, groups) == [1]
    after = [(alert, action) for alert, _, action in actions(records()) if action != "alert_change_consumed"]
    # Nothing is said about c: it is behind b and is handled again with it.
    assert after == [(a, "alert_indexed"), (b, "alert_index_deferred")]


def test_a_single_change_without_authority_fails_without_an_indexed_record(records, searchable):
    """Requirements: REV-74; observability 3 and 5. The consumer records the failure once."""
    a = str(UUID(int=7))
    searchable(set())
    with pytest.raises(AppError) as raised:
        consumer.index_record(None, None, None, hint(a, 10))
    assert raised.value.code == "search_hint_authority_missing"
    assert [action for _, _, action in actions(records())] == ["alert_change_consumed"]
