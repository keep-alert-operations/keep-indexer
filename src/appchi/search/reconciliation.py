"""Bounded authority scans repair lost hints, including deletion and manual edits.

A pass captures S before scanning permanent UUID identities. Identities added
behind its cursor have sequence > S. Changes after a scanned page also exceed S.
Only a completed, search-visible pass advances the lower-bound watermark to S.
"""

from collections import defaultdict
from contextlib import suppress
import logging
from time import monotonic
from uuid import uuid4

from appchi.cel.catalog import alert_name
from appchi.shared.errors import unavailable
from appchi.serialization import wire
from appchi.search.projection import Projection, build_document, canonical
from appchi.search.text import source_tokens
from appchi.search.writer import ProjectionWriter

# Authority scan page; hint batches name their identities and may reach the
# projection writer's per-bulk document limit instead.
PAGE_SIZE = 50
BULK_DOCUMENTS = 500
BULK_BYTES = 4 * 1024 * 1024
DEVELOPMENT_DOCUMENT_BYTES = 100 * 1024 * 1024
# Longest an idle scan goes without rereading due state from PostgreSQL.
IDLE_RECHECK_SECONDS = 1.0
log = logging.getLogger(__name__)


def current_page(
    conn,
    tenant,
    *,
    cursor=None,
    alert_id=None,
    alert_ids=None,
    floor_seq=0,
    ceiling_seq=None,
    max_document_bytes=DEVELOPMENT_DOCUMENT_BYTES,
):
    filters, params = ["tenant_id=%s", "current_projection_seq>%s"], [tenant, floor_seq]
    for clause, value in (
        ("current_projection_seq<=%s", ceiling_seq),
        ("id=%s", alert_id),
        ("id=ANY(%s::uuid[])", alert_ids),
    ):
        if value is not None:
            filters.append(clause)
            params.append(value)
    query = "SELECT * FROM alert_identities WHERE " + " AND ".join(filters)
    # A full pass or audit (floor_seq=0) walks the (tenant_id,id) index from
    # the cursor. A delta pass reads only identities changed after floor_seq
    # through the (tenant_id,current_projection_seq,id) index and sorts them:
    # sequence statistics are global, so an ordered id walk can visit every
    # unchanged identity of a tenant whose sequences are low; OFFSET 0 keeps
    # the subquery from being flattened into one. Unprepared execution plans
    # each page from its actual bounds, never a generic plan.
    if floor_seq:
        query = "SELECT * FROM (" + query + " OFFSET 0) changed"
    if cursor is not None:
        query += (" WHERE" if floor_seq else " AND") + " id>%s"
        params.append(cursor)
    # Only an explicit identity list keeps its caller's order.
    if alert_ids is not None:
        query += " ORDER BY array_position(%s::uuid[],id),id"
        params.append(alert_ids)
    else:
        query += " ORDER BY id"
    # A hint batch names its identities and may fill one writer bulk.
    limit = PAGE_SIZE if alert_ids is None else BULK_DOCUMENTS
    identities = conn.execute(query + " LIMIT %s", (*params, limit), prepare=False).fetchall()
    ids = [row["id"] for row in identities]
    if not ids:
        return []
    alerts = {
        row["id"]: row
        for row in conn.execute(
            """SELECT a.*,EXISTS(SELECT 1 FROM alert_notes n
                WHERE n.tenant_id=a.tenant_id AND n.alert_id=a.id AND NOT n.deleted) AS has_live_notes
                FROM alerts a WHERE a.tenant_id=%s AND a.id=ANY(%s::uuid[])""",
            (tenant, ids),
        )
    }
    links = defaultdict(list)
    for row in conn.execute(
        "SELECT alert_id,incident_id FROM incident_alert_links WHERE tenant_id=%s AND alert_id=ANY(%s::uuid[]) AND unlinked_at IS NULL ORDER BY incident_id",
        (tenant, ids),
    ):
        links[row["alert_id"]].append(str(row["incident_id"]))
    owners = {
        row["alert_id"]: str(row["incident_id"])
        for row in conn.execute(
            "SELECT alert_id,incident_id FROM alert_active_ownership WHERE tenant_id=%s AND alert_id=ANY(%s::uuid[])",
            (tenant, ids),
        )
    }
    # Live notes are streamed for the whole page through one named cursor.
    noted = [
        identity["id"]
        for identity in identities
        if not identity["retired_at"] and (alerts.get(identity["id"]) or {}).get("has_live_notes")
    ]
    with PageNotes(conn, tenant, noted) as notes:
        return page_items(identities, alerts, links, owners, notes, tenant, max_document_bytes)


class PageNotes:
    """Stream the live notes of a page's alerts through one named cursor.

    One statement replaces a server-side cursor per alert. Rows arrive grouped
    by alert in the page's identity order and by note id within an alert, so a
    caller reads each alert's notes in turn while the client holds at most one
    fetch batch. The cursor is opened only when a page alert has live notes.
    """

    ITERSIZE = 25

    def __init__(self, conn, tenant, alert_ids):
        self.conn, self.tenant, self.alert_ids = conn, tenant, list(alert_ids)
        self.position = {alert: index for index, alert in enumerate(self.alert_ids)}
        self.cursor = self.rows = self.pending = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if self.cursor is not None:
            self.cursor.close()

    def open(self):
        self.cursor = self.conn.cursor(name="projection_notes_" + uuid4().hex)
        self.cursor.itersize = self.ITERSIZE
        self.cursor.execute(*self.statement(self.tenant, self.alert_ids))
        self.rows = iter(self.cursor)

    @staticmethod
    def statement(tenant, ids):
        # Both forms read the partial (tenant_id,alert_id,id) notes index and
        # never sort the whole page. Cursor scans list identities in id order,
        # which one index scan over the id array returns directly.
        if ids == sorted(ids):
            return (
                "SELECT alert_id,text FROM alert_notes WHERE tenant_id=%s"
                " AND alert_id=ANY(%s::uuid[]) AND NOT deleted ORDER BY alert_id,id",
                (tenant, ids),
            )
        # A caller-ordered list (Kafka order) is walked alert by alert: a
        # nested loop probes the index per alert, so at most one alert's notes
        # are ordered at a time (an incremental sort presorted by position).
        return (
            "SELECT u.alert_id,n.text FROM unnest(%s::uuid[]) WITH ORDINALITY u(alert_id,ord)"
            " CROSS JOIN LATERAL (SELECT id,text FROM alert_notes WHERE tenant_id=%s"
            " AND alert_id=u.alert_id AND NOT deleted ORDER BY id) n ORDER BY u.ord,n.id",
            (ids, tenant),
        )

    def of(self, alert):
        """Yield one alert's live note texts in note-id order."""
        position = self.position[alert]
        if self.cursor is None:
            self.open()
        while True:
            if self.pending is None:
                self.pending = next(self.rows, None)
                if self.pending is None:
                    return
            row_position = self.position[self.pending["alert_id"]]
            if row_position > position:
                return
            row, self.pending = self.pending, None
            if row_position == position:
                yield row["text"]


def page_items(identities, alerts, links, owners, notes, tenant, max_document_bytes):
    items, page_bytes = [], 0
    for identity in identities:
        alert = identity["id"]
        if identity["retired_at"]:
            version = identity["final_version"]
            snapshot = {"id": str(alert), "tenant_id": str(tenant), "deleted": True}
            token_values = None
        else:
            row = alerts.get(alert)
            if not row:
                raise unavailable("search_authority_gap")
            has_live_notes = row.pop("has_live_notes")
            version = row["projection_version"]
            snapshot = dict(
                wire(row),
                live=True,
                last_received_at=wire(row["timestamp"]),
                notes=[],
                incident_ids=links[alert],
                active_incident_id=owners.get(alert),
            )
            # Keep only the distinct searchable tokens of this document,
            # bounded by its explicit integration allocation.
            # The same sources as build_document, including the effective name (REV-54).
            tokens = set(
                source_tokens([row["message"], row.get("impact") or "", alert_name(row) or ""])
            )
            token_bytes = sum(len(canonical(token)) + 1 for token in tokens)
            if has_live_notes:
                for text in notes.of(alert):
                    for token in source_tokens([text]):
                        if token not in tokens:
                            token_bytes += len(canonical(token)) + 1
                            if token_bytes > max_document_bytes:
                                raise unavailable("projection_document_capacity")
                            tokens.add(token)
            token_values = tuple(sorted(tokens))
        item = Projection(
            str(tenant),
            identity["current_projection_seq"],
            str(alert),
            version,
            snapshot,
            token_values,
        )
        encoded_size = len(canonical(build_document(item)))
        if encoded_size > max_document_bytes:
            raise unavailable("projection_document_capacity")
        # At most one large document plus a bounded ordinary page is assembled.
        # Leave an unselected identity for the next page; never advance over it.
        if items and page_bytes + encoded_size + 512 > BULK_BYTES:
            break
        items.append(item)
        page_bytes += encoded_size + 512
        if page_bytes >= BULK_BYTES:
            break
    return items


def publish_page(
    db,
    client,
    physical_index,
    epoch,
    items,
    *,
    fence=None,
    max_document_bytes=DEVELOPMENT_DOCUMENT_BYTES,
    on_write=None,
):
    from .control import check_writer

    if not items:
        return

    guarded = None

    def guard(item, document, candidate_epoch):
        nonlocal guarded
        if guarded is None:
            with db.transaction() as conn:
                if fence:
                    fence(conn)
                else:
                    check_writer(conn, physical_index, epoch)
                guarded = {
                    (str(row["tenant_id"]), str(row["id"])): row
                    for row in conn.execute(
                        "SELECT tenant_id,id,retired_at,final_version FROM alert_identities WHERE id=ANY(%s::uuid[])",
                        ([value.alert_id for value in selected],),
                    )
                }
        row = guarded.get((item.tenant_id, item.alert_id))
        if not row or (row["retired_at"] and item.version < row["final_version"]):
            raise unavailable("search_identity_changed")

    def newer(item, document):
        with db.transaction() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            rows = current_page(
                conn, item.tenant_id, alert_id=item.alert_id, max_document_bytes=max_document_bytes
            )
            return bool(
                rows and rows[0].version >= item.version and build_document(rows[0]) == document
            )

    # One byte-bounded bulk and one refresh-visible proof per turn. The caller
    # advances only this prefix, leaving the remainder available on the next turn.
    selected, size = [], 0
    for item in items[:BULK_DOCUMENTS]:
        item_bytes = len(canonical(build_document(item))) + 512
        if selected and size + item_bytes > BULK_BYTES:
            break
        selected.append(item)
        size += item_bytes
        if size >= BULK_BYTES:
            break
    writer = ProjectionWriter(client, physical_index, epoch, max_document_bytes=max_document_bytes)
    try:
        writer.publish(selected, guard=guard, covered_by_newer=newer)
    finally:
        # A bulk may have repaired documents even when its verification fails.
        if writer.written and on_write:
            on_write()
    return selected


def reconcile_page(
    db,
    client,
    physical_index,
    tenant,
    epoch,
    *,
    fence=None,
    advance_serving=True,
    max_document_bytes=DEVELOPMENT_DOCUMENT_BYTES,
    claim=None,
):
    from .control import check_writer

    with db.transaction() as conn:
        if fence:
            fence(conn)
        else:
            check_writer(conn, physical_index, epoch)
        conn.execute(
            "INSERT INTO search_reconciliation(index_epoch,tenant_id,boundary_seq) SELECT %s,tenant_id,projection_seq FROM tenant_work_state WHERE tenant_id=%s ON CONFLICT DO NOTHING",
            (epoch, tenant),
        )
        state = conn.execute(
            "SELECT * FROM search_reconciliation WHERE index_epoch=%s AND tenant_id=%s FOR UPDATE",
            (epoch, tenant),
        ).fetchone()
        if claim:
            check_claim(conn, epoch, tenant, claim)
        if state["completed_at"]:
            state = conn.execute(
                """UPDATE search_reconciliation r SET boundary_seq=w.projection_seq,
                    floor_seq=CASE WHEN %s AND r.full_audit_at IS NOT NULL
                        THEN coalesce(v.searchable_seq,0) ELSE 0 END,
                    cursor=NULL,completed_at=NULL,started_at=clock_timestamp()
                    FROM tenant_work_state w LEFT JOIN tenant_view_state v
                        ON v.tenant_id=w.tenant_id AND v.index_epoch=%s
                    WHERE r.index_epoch=%s AND r.tenant_id=%s AND w.tenant_id=r.tenant_id
                        RETURNING r.*""",
                (advance_serving, epoch, epoch, tenant),
            ).fetchone()
    with db.transaction() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        items = current_page(
            conn,
            tenant,
            cursor=state["cursor"],
            floor_seq=state["floor_seq"],
            max_document_bytes=max_document_bytes,
        )

    def publication_fence(conn):
        if fence:
            fence(conn)
        else:
            check_writer(conn, physical_index, epoch)
        if claim:
            check_claim(conn, epoch, tenant, claim)

    items = (
        publish_page(
            db,
            client,
            physical_index,
            epoch,
            items,
            fence=publication_fence,
            max_document_bytes=max_document_bytes,
        )
        or []
    )
    with db.transaction() as conn:
        if fence:
            fence(conn)
        else:
            check_writer(conn, physical_index, epoch)
        current = conn.execute(
            "SELECT * FROM search_reconciliation WHERE index_epoch=%s AND tenant_id=%s FOR UPDATE",
            (epoch, tenant),
        ).fetchone()
        if claim:
            check_claim(conn, epoch, tenant, claim)
        if (
            current["boundary_seq"],
            current["cursor"],
            current["started_at"],
            current["floor_seq"],
        ) != (
            state["boundary_seq"],
            state["cursor"],
            state["started_at"],
            state["floor_seq"],
        ):
            return False
        if items:
            conn.execute(
                """UPDATE search_reconciliation SET cursor=%s,last_page_at=clock_timestamp(),
                    audit_turns=least(audit_turns+1,4) WHERE index_epoch=%s AND tenant_id=%s""",
                (items[-1].alert_id, epoch, tenant),
            )
            return False
        conn.execute(
            """UPDATE search_reconciliation SET completed_at=clock_timestamp(),
                last_page_at=clock_timestamp(),
                audit_turns=least(audit_turns+1,4),
                full_audit_at=CASE WHEN floor_seq=0 THEN clock_timestamp() ELSE full_audit_at END
                WHERE index_epoch=%s AND tenant_id=%s""",
            (epoch, tenant),
        )
        if advance_serving:
            # Every view-state update fires a live hint. An idle pass that neither
            # advances, verifies nor completes the first full audit leaves it alone.
            conn.execute(
                """UPDATE tenant_view_state SET searchable_seq=greatest(searchable_seq,%s),
                    verified_at=clock_timestamp(),view_generation=view_generation+1
                    WHERE tenant_id=%s AND index_epoch=%s
                        AND (searchable_seq<%s OR verified_at IS NULL OR %s)""",
                (
                    state["boundary_seq"],
                    tenant,
                    epoch,
                    state["boundary_seq"],
                    state["full_audit_at"] is None,
                ),
            )
        return True


def audit_page(db, client, physical_index, tenant, epoch, *, max_document_bytes, claim):
    """Repair a finite older generation without delaying ordinary watermarks."""
    from .control import check_writer

    def fence(conn):
        check_writer(conn, physical_index, epoch)
        check_claim(conn, epoch, tenant, claim)

    with db.transaction() as conn:
        fence(conn)
        state = conn.execute(
            "SELECT * FROM search_reconciliation WHERE index_epoch=%s AND tenant_id=%s FOR UPDATE",
            (epoch, tenant),
        ).fetchone()
        check_claim(conn, epoch, tenant, claim)
        if state["audit_boundary_seq"] is None:
            state = conn.execute(
                """UPDATE search_reconciliation SET audit_boundary_seq=
                    (SELECT projection_seq FROM tenant_work_state WHERE tenant_id=%s),
                    audit_cursor=NULL,audit_started_at=clock_timestamp()
                    WHERE index_epoch=%s AND tenant_id=%s RETURNING *""",
                (tenant, epoch, tenant),
            ).fetchone()
    with db.transaction() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        items = current_page(
            conn,
            tenant,
            cursor=state["audit_cursor"],
            ceiling_seq=state["audit_boundary_seq"],
            max_document_bytes=max_document_bytes,
        )
    repaired = []

    def announce(conn):
        # Repairs leave the watermark alone, but open feeds must still refetch.
        conn.execute(
            "UPDATE tenant_view_state SET view_generation=view_generation+1 WHERE tenant_id=%s AND index_epoch=%s",
            (tenant, epoch),
        )

    try:
        items = (
            publish_page(
                db,
                client,
                physical_index,
                epoch,
                items,
                fence=fence,
                max_document_bytes=max_document_bytes,
                on_write=lambda: repaired.append(True),
            )
            or []
        )
        with db.transaction() as conn:
            fence(conn)
            current = conn.execute(
                "SELECT * FROM search_reconciliation WHERE index_epoch=%s AND tenant_id=%s FOR UPDATE",
                (epoch, tenant),
            ).fetchone()
            check_claim(conn, epoch, tenant, claim)
            if repaired:
                announce(conn)
            fields = ("audit_boundary_seq", "audit_cursor", "audit_started_at")
            if any(current[field] != state[field] for field in fields):
                return False
            if items:
                conn.execute(
                    """UPDATE search_reconciliation SET audit_cursor=%s,
                    audit_page_at=clock_timestamp(),last_page_at=clock_timestamp(),audit_turns=0
                    WHERE index_epoch=%s AND tenant_id=%s""",
                    (items[-1].alert_id, epoch, tenant),
                )
                return False
            conn.execute(
                """UPDATE search_reconciliation SET audit_cursor=NULL,audit_boundary_seq=NULL,
                audit_started_at=NULL,audit_page_at=clock_timestamp(),last_page_at=clock_timestamp(),
                full_audit_at=clock_timestamp(),audit_turns=0 WHERE index_epoch=%s AND tenant_id=%s""",
                (epoch, tenant),
            )
            return True
    except Exception:
        # A retry finds the repaired documents visible and would never announce
        # them. Without the claim, announce them as long as this index is current.
        if repaired:
            with suppress(Exception), db.transaction() as conn:
                check_writer(conn, physical_index, epoch)
                announce(conn)
        raise


def check_claim(conn, epoch, tenant, owner):
    if not conn.execute(
        "SELECT 1 FROM search_reconciliation WHERE index_epoch=%s AND tenant_id=%s AND lease_owner=%s AND lease_until>clock_timestamp()",
        (epoch, tenant, owner),
    ).fetchone():
        raise unavailable("search_reconciliation_fenced")


def document_allocation(settings, profile):
    value = (profile or {}).get("search_max_document_bytes")
    if value is None and settings.environment == "development":
        return DEVELOPMENT_DOCUMENT_BYTES
    if type(value) is not int or not 1024 <= value <= DEVELOPMENT_DOCUMENT_BYTES:
        raise unavailable("search_document_capacity_profile_required")
    return value


class ProjectionService:
    """Process-owned HTTP connection pool shared by hints and authority scans."""

    def __init__(self, db, settings, profile=None):
        from elasticsearch import Elasticsearch
        from appchi.service_clients import elasticsearch_options

        self.db, self.settings = db, settings
        self.max_document_bytes = document_allocation(settings, profile)
        self.client = Elasticsearch(settings.elasticsearch_url, **elasticsearch_options(settings))

    def close(self):
        self.client.close()

    def project(self, tenant, alert_ids):
        return {
            alert
            for _, alert in self.project_many([(str(tenant), str(alert)) for alert in alert_ids])
        }

    def project_many(self, requests):
        from .control import writer_lease

        tenants = {}
        for tenant, alert in requests:
            tenants.setdefault(tenant, []).append(alert)
        with writer_lease(self.db, self.settings) as physical_index:
            items, size, epoch = [], 0, None
            with self.db.transaction() as conn:
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                for tenant, alert_ids in tenants.items():
                    candidate_epoch = conn.execute(
                        "SELECT index_epoch FROM tenant_view_state WHERE tenant_id=%s", (tenant,)
                    ).fetchone()["index_epoch"]
                    if epoch is not None and candidate_epoch != epoch:
                        break
                    epoch = candidate_epoch
                    page = current_page(
                        conn,
                        tenant,
                        alert_ids=alert_ids,
                        max_document_bytes=self.max_document_bytes,
                    )
                    full = False
                    for item in page:
                        item_bytes = len(canonical(build_document(item))) + 512
                        if items and size + item_bytes > BULK_BYTES:
                            full = True
                            break
                        items.append(item)
                        size += item_bytes
                    if full or size >= BULK_BYTES or len(items) >= BULK_DOCUMENTS:
                        break
            published = (
                publish_page(
                    self.db,
                    self.client,
                    physical_index,
                    epoch,
                    items,
                    max_document_bytes=self.max_document_bytes,
                )
                or []
            )
            return {(item.tenant_id, item.alert_id) for item in published}

    # Monotonic time before which no tenant can be due; 0 means check now.
    idle_until = 0.0

    def idle_seconds(self):
        """Seconds until the next due check; 0 while work may be due."""
        return max(0.0, self.idle_until - monotonic())

    def next_due(self):
        """Seconds until a tenant becomes claimable, read without a writer lease.

        Mirrors the claim predicate below. Returns 0 when a claim may succeed now
        (including view states without a reconciliation row) and None when no
        reconciliation row exists (the statement's empty case yields NULL). A closed search gate fails as the claim would;
        writer fences (paused writers, epoch or binding changes) are still
        enforced by the writer lease that every claim takes.
        """
        with self.db.transaction(read_committed=True) as conn:
            row = conn.execute(
                """SELECT (SELECT ready FROM capability_gates WHERE capability='search') AS ready,
                    EXISTS(SELECT 1 FROM tenant_view_state v
                        JOIN tenant_work_state w ON w.tenant_id=v.tenant_id
                        WHERE NOT EXISTS(SELECT 1 FROM search_reconciliation r
                            WHERE r.index_epoch=v.index_epoch AND r.tenant_id=v.tenant_id))
                        AS unregistered,
                    (SELECT CASE WHEN count(*)=0 THEN NULL WHEN bool_or(d.due_at IS NULL) THEN 0
                        ELSE greatest(0,extract(epoch FROM min(d.due_at)-clock_timestamp())) END
                    FROM (SELECT greatest(
                        CASE WHEN r.completed_at IS NULL OR r.audit_started_at IS NOT NULL
                            THEN NULL ELSE r.completed_at+interval '2 seconds' END,
                        r.lease_until,r.retry_after) AS due_at
                        FROM search_reconciliation r JOIN tenant_view_state v
                            ON v.tenant_id=r.tenant_id AND v.index_epoch=r.index_epoch) d) AS due_in"""
            ).fetchone()
        if not row["ready"]:
            raise unavailable("search_not_ready")
        if row["unregistered"]:
            return 0.0
        return None if row["due_in"] is None else float(row["due_in"])

    def reconcile(self):
        # An idle turn issues no statement until the next due time, bounded so
        # new tenants, epochs and gate changes are seen within IDLE_RECHECK_SECONDS.
        if monotonic() < self.idle_until:
            return 0
        self.idle_until = 0.0
        wait = self.next_due()
        if wait is None or wait > 0:
            self.idle_until = monotonic() + (
                IDLE_RECHECK_SECONDS if wait is None else min(IDLE_RECHECK_SECONDS, wait)
            )
            return 0
        return self.claim_and_reconcile()

    def claim_and_reconcile(self):
        from .control import writer_lease

        owner = uuid4()
        with writer_lease(self.db, self.settings) as physical_index:
            with self.db.transaction() as conn:
                gate = conn.execute(
                    "SELECT ready FROM capability_gates WHERE capability='search'"
                ).fetchone()
                if not gate or not gate["ready"]:
                    raise unavailable("search_not_ready")
                conn.execute("""INSERT INTO search_reconciliation(index_epoch,tenant_id,boundary_seq)
                    SELECT v.index_epoch,v.tenant_id,w.projection_seq FROM tenant_view_state v
                    JOIN tenant_work_state w ON w.tenant_id=v.tenant_id
                    WHERE NOT EXISTS(SELECT 1 FROM search_reconciliation r
                        WHERE r.index_epoch=v.index_epoch AND r.tenant_id=v.tenant_id)
                    ON CONFLICT DO NOTHING""")
                row = conn.execute(
                    """WITH candidate AS (
                    SELECT r.index_epoch,r.tenant_id FROM search_reconciliation r
                    JOIN tenant_view_state v ON v.tenant_id=r.tenant_id AND v.index_epoch=r.index_epoch
                    WHERE (r.completed_at IS NULL OR r.completed_at<=clock_timestamp()-interval '2 seconds'
                        OR r.audit_started_at IS NOT NULL)
                      AND (r.lease_until IS NULL OR r.lease_until<=clock_timestamp())
                      AND (r.retry_after IS NULL OR r.retry_after<=clock_timestamp())
                    ORDER BY r.last_page_at NULLS FIRST,r.tenant_id
                    FOR UPDATE OF r SKIP LOCKED LIMIT 1)
                    UPDATE search_reconciliation r SET lease_owner=%s,
                        lease_until=clock_timestamp()+interval '30 seconds'
                    FROM candidate c WHERE r.index_epoch=c.index_epoch AND r.tenant_id=c.tenant_id
                    RETURNING r.tenant_id,r.index_epoch,
                        (r.full_audit_at IS NOT NULL AND r.audit_turns>=4 AND
                            (r.audit_retry_after IS NULL OR r.audit_retry_after<=clock_timestamp()) AND
                            (r.audit_started_at IS NOT NULL OR r.full_audit_at<=clock_timestamp()-interval '5 minutes')) AS audit""",
                    (owner,),
                ).fetchone()
            if not row:
                return 0
            try:
                (audit_page if row.get("audit") else reconcile_page)(
                    self.db,
                    self.client,
                    physical_index,
                    row["tenant_id"],
                    row["index_epoch"],
                    max_document_bytes=self.max_document_bytes,
                    claim=owner,
                )
            except Exception as error:
                # Store only controlled diagnostics, never service exception text.
                code = getattr(error, "code", "search_reconciliation_failed")
                allowed = {
                    "projection_document_capacity",
                    "search_authority_gap",
                    "projection_version_conflict",
                    "search_reconciliation_fenced",
                }
                code = code if code in allowed else "search_reconciliation_failed"
                with self.db.transaction() as conn:
                    if row.get("audit"):
                        conn.execute(
                            """UPDATE search_reconciliation SET lease_owner=NULL,lease_until=NULL,
                                audit_retry_after=clock_timestamp()+interval '5 seconds',audit_last_error=%s,
                                audit_turns=0,last_page_at=clock_timestamp()
                                WHERE index_epoch=%s AND tenant_id=%s AND lease_owner=%s""",
                            (code, row["index_epoch"], row["tenant_id"], owner),
                        )
                    else:
                        conn.execute(
                            """UPDATE search_reconciliation SET lease_owner=NULL,lease_until=NULL,
                        retry_after=clock_timestamp()+interval '5 seconds',last_error=%s,
                        last_page_at=clock_timestamp()
                        WHERE index_epoch=%s AND tenant_id=%s AND lease_owner=%s""",
                            (code, row["index_epoch"], row["tenant_id"], owner),
                        )
                log.warning("search_tenant_reconciliation_failed reason=%s", code)
                return 0
            with self.db.transaction() as conn:
                cleared = (
                    "audit_retry_after=NULL,audit_last_error=NULL"
                    if row.get("audit")
                    else "retry_after=NULL,last_error=NULL"
                )
                conn.execute(
                    "UPDATE search_reconciliation SET lease_owner=NULL,lease_until=NULL,"
                    + cleared
                    + " WHERE index_epoch=%s AND tenant_id=%s AND lease_owner=%s",
                    (row["index_epoch"], row["tenant_id"], owner),
                )
            return 1


def service_for(db, settings, profile=None):
    service = getattr(db, "_appchi_projection_service", None)
    if service is None:
        service = ProjectionService(db, settings, profile)
        db._appchi_projection_service = service
    return service


def project_alert(db, settings, tenant, alert_id):
    return service_for(db, settings).project(tenant, [alert_id])


class ReconciliationRunner:
    def __init__(self):
        self.service = None

    def __call__(self, db, settings):
        import json

        if self.service is None:
            self.service = getattr(db, "_appchi_projection_service", None)
        if self.service is None:
            profile = (
                json.loads(settings.capacity_profile.read_text())
                if settings.capacity_profile
                else None
            )
            self.service = service_for(db, settings, profile)
        return self.service.reconcile()

    def idle_seconds(self):
        """Let the worker loop sleep until the next due check when it has no poll."""
        return self.service.idle_seconds() if self.service else 0.0

    def close(self):
        if self.service:
            self.service.close()


def reconciliation_turn(db, settings):
    return service_for(db, settings).reconcile()
