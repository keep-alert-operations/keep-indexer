"""One named cursor streams a whole page's live notes, grouped per alert."""

from datetime import UTC, datetime
from uuid import UUID

import pytest

from keep.search import reconciliation
from keep.search.projection import Projection
from keep.search.text import source_tokens
from keep.shared.errors import AppError

TENANT = UUID(int=7)
NOW = datetime(2026, 10, 6, tzinfo=UTC)


def uid(value):
    return UUID(int=value)


class Result(list):
    def fetchall(self):
        return list(self)


class NamedCursor:
    """Server-side cursor over an in-memory alert_notes table."""

    def __init__(self, conn, name):
        self.conn, self.name = conn, name
        self.statements, self.fetched, self.closed = [], 0, False
        self.itersize = None

    def execute(self, query, params):
        self.statements.append((query, params))
        if "WITH ORDINALITY" in query:
            order, tenant = list(params[0]), params[1]
        else:
            tenant, order = params[0], None
        ids = set(order if order is not None else params[1])
        rows = [
            note
            for note in self.conn.notes
            if note["tenant_id"] == tenant and note["alert_id"] in ids and not note["deleted"]
        ]
        if order is not None:
            rows.sort(key=lambda note: (order.index(note["alert_id"]), note["id"]))
        else:
            rows.sort(key=lambda note: (note["alert_id"], note["id"]))
        self.rows = [{"alert_id": note["alert_id"], "text": note["text"]} for note in rows]

    def __iter__(self):
        for row in self.rows:
            self.fetched += 1
            yield row

    def close(self):
        self.closed = True


class Conn:
    def __init__(self, identities, alerts, notes):
        self.identities, self.alerts, self.notes = identities, alerts, notes
        self.cursors = []

    def cursor(self, name=None):
        assert name, "notes must use a server-side cursor"
        cursor = NamedCursor(self, name)
        self.cursors.append(cursor)
        return cursor

    def execute(self, query, params, **_options):
        # Accepts execute options such as prepare=False from current_page.
        if "FROM alert_identities" in query:
            # An explicit identity list keeps the caller's order; scans use id order.
            requested = [value for value in params if isinstance(value, list)]
            if requested:
                order = requested[-1]
                return Result(
                    sorted(
                        (row for row in self.identities if row["id"] in order),
                        key=lambda row: order.index(row["id"]),
                    )
                )
            return Result(sorted(self.identities, key=lambda row: row["id"]))
        if "FROM alerts a" in query:
            ids = set(params[1])
            return Result(
                dict(
                    row,
                    has_live_notes=any(
                        note["alert_id"] == row["id"] and not note["deleted"] for note in self.notes
                    ),
                )
                for row in self.alerts
                if row["id"] in ids
            )
        return Result()


def identity(alert, retired=False):
    return {
        "id": alert,
        "retired_at": NOW if retired else None,
        "final_version": 3 if retired else None,
        "current_projection_seq": alert.int,
    }


def alert_row(alert):
    return {
        "id": alert,
        "tenant_id": TENANT,
        "schema_version": "v1",
        "key_field": "key",
        "operator": "legacy",
        "application": "app",
        "component": "cpu",
        "node_name": None,
        "site": None,
        "impact": None,
        "runbook_url": None,
        "message": f"message{alert.int}",
        "severity": "critical",
        "status": "firing",
        "suppressed": False,
        "acknowledged": False,
        "time_created": NOW,
        "first_received_at": NOW,
        "timestamp": NOW,
        "occurrence_id": uid(9),
        "occurrence_number": 1,
        "opened_at": NOW,
        "resolved_at": None,
        "acknowledged_by": None,
        "acknowledged_at": None,
        "assignee_id": None,
        "revision": 1,
        "projection_version": 1,
    }


def note(number, alert, text, deleted=False, tenant=TENANT):
    return {
        "id": uid(1000 + number),
        "tenant_id": tenant,
        "alert_id": alert,
        "text": text,
        "deleted": deleted,
    }


def scenario():
    alerts = [uid(value) for value in (5, 1, 4, 2, 3)]
    notes = [
        # Inserted out of id order and interleaved across alerts.
        note(9, alerts[0], "fifthlater"),
        note(2, alerts[1], "firstearly"),
        note(4, alerts[2], "deletedonly", deleted=True),
        note(1, alerts[0], "fifthearly"),
        note(8, alerts[1], "firstlater"),
        note(3, alerts[3], "secondonly"),
        note(5, alerts[1], "firstdeleted", deleted=True),
        note(6, alerts[4], "retiredtoken"),
        note(7, alerts[0], "othertenant", tenant=uid(8)),
    ]
    identities = [identity(alert, retired=alert == alerts[4]) for alert in alerts]
    rows = [alert_row(alert) for alert in alerts if alert != alerts[4]]
    return alerts, identities, rows, notes


def expected_tokens(row, notes):
    """The previous per-alert cursor: live notes of one alert in id order."""
    tokens = set(source_tokens([row["message"], row.get("impact") or ""]))
    for value in sorted(notes, key=lambda value: value["id"]):
        if value["tenant_id"] == TENANT and value["alert_id"] == row["id"] and not value["deleted"]:
            tokens.update(source_tokens([value["text"]]))
    return tuple(sorted(tokens))


def test_page_reads_all_notes_through_one_statement_with_identical_grouping():
    alerts, identities, rows, notes = scenario()
    conn = Conn(identities, rows, notes)
    page = reconciliation.current_page(conn, TENANT)
    assert len(conn.cursors) == 1
    (cursor,) = conn.cursors
    assert len(cursor.statements) == 1 and cursor.closed and cursor.itersize == 25
    query, params = cursor.statements[0]
    assert "alert_id=ANY(" in query and query.endswith("ORDER BY alert_id,id")
    assert "NOT deleted" in query and params[0] == TENANT
    # Only live alerts whose notes are live are requested, in page order.
    assert params[1] == [uid(1), uid(2), uid(5)]
    assert [item.alert_id for item in page] == [str(uid(value)) for value in (1, 2, 3, 4, 5)]
    by_id = {UUID(item.alert_id): item for item in page}
    for row in rows:
        assert by_id[row["id"]].tokens == expected_tokens(row, notes)
    assert "deletedonly" not in by_id[uid(4)].tokens
    assert "firstdeleted" not in by_id[uid(1)].tokens
    assert "othertenant" not in by_id[uid(5)].tokens
    assert by_id[uid(3)].tokens is None
    assert by_id[uid(3)].snapshot["deleted"] is True


def test_requested_page_keeps_caller_order_with_one_statement():
    alerts, identities, rows, notes = scenario()
    conn = Conn(identities, rows, notes)
    requested = [uid(5), uid(2), uid(1), uid(4)]
    page = reconciliation.current_page(conn, TENANT, alert_ids=requested)
    assert [UUID(item.alert_id) for item in page] == requested
    (cursor,) = conn.cursors
    ((query, params),) = cursor.statements
    # Walked alert by alert in caller order, never sorted as a whole page.
    assert "unnest(%s::uuid[]) WITH ORDINALITY" in query
    assert "tenant_id=%s AND alert_id=u.alert_id AND NOT deleted ORDER BY id" in query
    assert query.endswith("ORDER BY u.ord,n.id") and "array_position" not in query
    assert params == ([uid(5), uid(2), uid(1)], TENANT)
    by_id = {UUID(item.alert_id): item for item in page}
    for row in rows:
        assert by_id[row["id"]].tokens == expected_tokens(row, notes)


def test_page_without_live_notes_opens_no_cursor():
    alert = uid(1)
    conn = Conn([identity(alert)], [alert_row(alert)], [note(1, alert, "gone", deleted=True)])
    (item,) = reconciliation.current_page(conn, TENANT)
    assert conn.cursors == []
    assert item.tokens == tuple(sorted(source_tokens(["message1"])))


def test_notes_of_one_alert_stream_in_note_id_order():
    first, second = uid(1), uid(2)
    notes = [
        note(3, first, "c"),
        note(1, second, "x"),
        note(2, first, "b"),
        note(0, first, "a"),
    ]
    conn = Conn([], [], notes)
    with reconciliation.PageNotes(conn, TENANT, [first, second]) as page_notes:
        assert list(page_notes.of(first)) == ["a", "b", "c"]
        assert conn.cursors[0].fetched == 4  # One row of lookahead stays pending.
        assert list(page_notes.of(second)) == ["x"]
    assert conn.cursors[0].closed


def test_note_capacity_is_still_enforced_per_document():
    alert = uid(1)
    notes = [note(i, alert, f"token{i:04d}") for i in range(200)]
    conn = Conn([identity(alert)], [alert_row(alert)], notes)
    with pytest.raises(AppError) as failure:
        reconciliation.current_page(conn, TENANT, max_document_bytes=500)
    assert failure.value.code == "projection_document_capacity"
    # Streaming stopped once the cap was crossed and the cursor was closed.
    (cursor,) = conn.cursors
    assert cursor.fetched < len(notes) and cursor.closed


def test_page_byte_bound_stops_streaming_later_alerts(monkeypatch):
    alerts = [uid(value) for value in range(1, 6)]
    notes = [
        note(index * 100 + j, alert, f"a{index}n{j}")
        for index, alert in enumerate(alerts)
        for j in range(30)
    ]
    conn = Conn([identity(alert) for alert in alerts], [alert_row(a) for a in alerts], notes)
    monkeypatch.setattr(reconciliation, "BULK_BYTES", 1)
    page = reconciliation.current_page(conn, TENANT)
    # One document fills the page; later alerts' notes are never fetched.
    assert [item.alert_id for item in page] == [str(alerts[0])]
    (cursor,) = conn.cursors
    assert cursor.fetched == 31 and cursor.closed
    assert isinstance(page[0], Projection)


def test_caller_ordered_notes_stream_per_alert_in_note_id_order():
    first, second = uid(1), uid(2)
    notes = [note(3, first, "c"), note(1, second, "x"), note(2, first, "b"), note(0, second, "w")]
    conn = Conn([], [], notes)
    with reconciliation.PageNotes(conn, TENANT, [second, first]) as page_notes:
        assert list(page_notes.of(second)) == ["w", "x"]
        assert list(page_notes.of(first)) == ["b", "c"]
    ((query, params),) = conn.cursors[0].statements
    assert "WITH ORDINALITY" in query and params == ([second, first], TENANT)
