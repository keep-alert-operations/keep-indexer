"""An idle reconciliation loop takes no writer lease and no claim until work is due."""

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from keep.search import control, reconciliation
from keep.shared.errors import AppError, unavailable


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class Database:
    """Record every statement; answer the due check from ``self.due``."""

    def __init__(self):
        self.statements = []
        self.due = {"ready": True, "unregistered": False, "due_in": 1.5}
        self.claim = {"tenant_id": "tenant", "index_epoch": "epoch", "audit": False}

    @contextmanager
    def transaction(self, *args, **kwargs):
        db = self

        class Connection:
            def execute(self, sql, params=()):
                db.statements.append(sql)
                if "AS due_in" in sql:
                    return Mock(fetchone=lambda: dict(db.due))
                if "SELECT ready" in sql:
                    return Mock(fetchone=lambda: {"ready": db.due["ready"]})
                if "RETURNING r.tenant_id" in sql:
                    return Mock(fetchone=lambda: db.claim)
                return Mock()

        yield Connection()

    def count(self, fragment):
        return sum(fragment in sql for sql in self.statements)


@pytest.fixture
def idle(monkeypatch):
    clock, db = Clock(), Database()
    leases = []

    @contextmanager
    def lease(*args):
        leases.append("insert")
        if getattr(db, "paused", False):
            raise unavailable("projection_fenced")
        try:
            yield "physical-index"
        finally:
            leases.append("delete")

    page = Mock(return_value=False)
    monkeypatch.setattr(reconciliation, "monotonic", clock)
    monkeypatch.setattr(control, "writer_lease", lease)
    monkeypatch.setattr(reconciliation, "reconcile_page", page)
    service = reconciliation.ProjectionService.__new__(reconciliation.ProjectionService)
    service.db, service.settings, service.client = db, Mock(), Mock()
    service.max_document_bytes = 2000000
    return SimpleNamespace(clock=clock, db=db, leases=leases, page=page, service=service)


def test_idle_turns_take_no_lease_or_claim_until_the_next_due_time(idle):
    assert idle.service.reconcile() == 0
    assert idle.db.count("AS due_in") == 1
    statements = len(idle.db.statements)
    # The worker loop calls about every 110 ms while its Kafka poll is idle.
    for _ in range(9):
        idle.clock.now += 0.11
        assert idle.service.reconcile() == 0
    assert len(idle.db.statements) == statements  # No statement at all.
    assert idle.leases == []
    assert not idle.db.count("SKIP LOCKED") and not idle.db.count(
        "INSERT INTO search_reconciliation"
    )
    assert idle.service.idle_seconds() == pytest.approx(0.01)
    # One bounded recheck: still 0.5 s to go, so wait for it without a claim.
    idle.clock.now += 0.01
    idle.db.due["due_in"] = 0.5
    assert idle.service.reconcile() == 0
    assert idle.db.count("AS due_in") == 2 and idle.leases == []
    idle.clock.now += 0.49
    assert idle.service.reconcile() == 0
    assert idle.db.count("AS due_in") == 2
    # Due: the check wakes at the due time and the unchanged claim path runs.
    idle.clock.now += 0.01
    idle.db.due["due_in"] = 0
    assert idle.service.reconcile() == 1
    assert idle.leases == ["insert", "delete"]
    assert idle.db.count("SKIP LOCKED") == 1
    assert idle.page.call_args.args[3:5] == ("tenant", "epoch")
    assert idle.page.call_args.kwargs["claim"] is not None


def test_recheck_is_bounded_when_nothing_is_registered_or_due_later(idle):
    idle.db.due["due_in"] = None  # No reconciliation rows: the statement yields NULL.
    assert idle.service.reconcile() == 0
    assert idle.service.idle_seconds() == reconciliation.IDLE_RECHECK_SECONDS
    idle.db.due["due_in"] = 30.0  # A claim held by another replica.
    idle.clock.now += reconciliation.IDLE_RECHECK_SECONDS
    assert idle.service.reconcile() == 0
    assert idle.service.idle_seconds() == reconciliation.IDLE_RECHECK_SECONDS
    # A tenant provisioned meanwhile has no row yet and is claimed at the recheck.
    idle.db.due.update(unregistered=True)
    idle.clock.now += reconciliation.IDLE_RECHECK_SECONDS
    assert idle.service.reconcile() == 1
    assert idle.db.count("INSERT INTO search_reconciliation") == 1


def test_work_after_a_pass_is_checked_on_the_next_turn(idle):
    idle.db.due["due_in"] = 0
    assert idle.service.reconcile() == 1
    # An unfinished pass is due again at once: no idle wait after progress.
    assert idle.service.idle_seconds() == 0
    assert idle.service.reconcile() == 1
    assert idle.db.count("AS due_in") == 2 and idle.page.call_count == 2


def test_lost_claim_race_is_rechecked_without_waiting(idle):
    idle.db.due["due_in"] = 0
    idle.db.claim = None  # Another replica claimed the due tenant first.
    assert idle.service.reconcile() == 0
    assert idle.leases == ["insert", "delete"]
    assert idle.service.idle_seconds() == 0


def test_closed_gate_fails_without_a_lease_and_is_not_cached(idle):
    idle.db.due["ready"] = False
    for _ in range(2):
        with pytest.raises(AppError) as failure:
            idle.service.reconcile()
        assert failure.value.code == "search_not_ready"
    assert idle.db.count("AS due_in") == 2 and idle.leases == []


def test_paused_writers_still_fence_due_work_on_every_turn(idle):
    idle.db.due["due_in"] = 0
    idle.db.paused = True
    for _ in range(2):
        with pytest.raises(AppError) as failure:
            idle.service.reconcile()
        assert failure.value.code == "projection_fenced"
    assert idle.page.call_count == 0 and not idle.db.count("SKIP LOCKED")
    assert idle.db.count("AS due_in") == 2
    idle.db.paused = False
    assert idle.service.reconcile() == 1


def test_runner_reports_the_service_idle_time(idle):
    runner = reconciliation.ReconciliationRunner()
    assert runner.idle_seconds() == 0
    runner.service = idle.service
    assert runner(idle.db, Mock()) == 0
    assert runner.idle_seconds() == reconciliation.IDLE_RECHECK_SECONDS  # 1.5 s, bounded.
