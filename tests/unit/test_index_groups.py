"""Changed hints from several partitions share one projection request."""

from types import SimpleNamespace
from uuid import UUID, uuid4

from keep.search import consumer


def hint(tenant, alert, *, key=None, value=None):
    import json

    body = (
        value
        or json.dumps(
            {
                "event_id": str(uuid4()),
                "tenant_id": str(tenant),
                "alert_id": str(alert),
            }
        ).encode()
    )
    return SimpleNamespace(
        value=lambda: body, key=lambda: key if key is not None else UUID(str(tenant)).bytes
    )


class Envelope:
    @staticmethod
    def model_validate(data):
        return SimpleNamespace(tenant_id=data["tenant_id"], alert_id=data["alert_id"])


def service(monkeypatch, covered=None):
    calls = []

    def project_many(requests):
        calls.append(list(requests))
        return set(requests) if covered is None else covered(requests)

    monkeypatch.setattr(consumer, "ChangedEnvelope", Envelope)
    monkeypatch.setattr(
        consumer, "service_for", lambda *args: SimpleNamespace(project_many=project_many)
    )
    return calls


def test_all_groups_go_into_one_projection_request_in_group_order(monkeypatch):
    calls = service(monkeypatch)
    t1, t2 = uuid4(), uuid4()
    a, b, c = uuid4(), uuid4(), uuid4()
    groups = [[hint(t1, a), hint(t1, b), hint(t1, a)], [hint(t2, c), hint(t1, b)]]
    assert consumer.index_groups(None, None, None, groups) == [3, 2]
    # One request, deduplicated, so one bulk and one refresh wait serve both.
    assert calls == [[(str(t1), str(a)), (str(t1), str(b)), (str(t2), str(c))]]


def test_each_group_counts_only_its_covered_prefix(monkeypatch):
    t = uuid4()
    a, b, c = uuid4(), uuid4(), uuid4()
    service(monkeypatch, covered=lambda requests: {(str(t), str(a)), (str(t), str(c))})
    groups = [[hint(t, a), hint(t, b), hint(t, c)], [hint(t, b)], [hint(t, c), hint(t, a)]]
    assert consumer.index_groups(None, None, None, groups) == [1, 0, 2]


def test_a_bad_hint_stops_only_its_own_group(monkeypatch):
    calls = service(monkeypatch)
    t, other = uuid4(), uuid4()
    a, b = uuid4(), uuid4()
    groups = [
        [hint(t, a), hint(t, b, key=other.bytes), hint(t, b)],
        [hint(t, b, value=b"not json")],
        [hint(t, b)],
    ]
    assert consumer.index_groups(None, None, None, groups) == [1, 0, 1]
    assert calls == [[(str(t), str(a)), (str(t), str(b))]]
