"""Bounded authority scans repair lost hints, including deletion and manual edits.

A pass captures S before scanning permanent UUID identities. Identities added
behind its cursor have sequence > S. Changes after a scanned page also exceed S.
Only a completed, search-visible pass advances the lower-bound watermark to S.
"""

from collections import defaultdict
from time import monotonic

from appchi.shared.errors import unavailable
from appchi.serialization import wire
from appchi.search.projection import Projection, build_document
from appchi.search.writer import ProjectionWriter

PAGE_SIZE = 50


def current_page(conn, tenant, *, cursor=None, alert_id=None):
    identities = conn.execute(
        """SELECT * FROM alert_identities WHERE tenant_id=%s
        AND (%s::uuid IS NULL OR id>%s) AND (%s::uuid IS NULL OR id=%s)
        AND current_projection_seq>0 ORDER BY id LIMIT %s""",
        (tenant, cursor, cursor, alert_id, alert_id, PAGE_SIZE),
    ).fetchall()
    ids = [row["id"] for row in identities]
    if not ids:
        return []
    alerts = {
        row["id"]: row
        for row in conn.execute(
            "SELECT * FROM alerts WHERE tenant_id=%s AND id=ANY(%s::uuid[])", (tenant, ids)
        )
    }
    notes, links = defaultdict(list), defaultdict(list)
    for row in conn.execute(
        "SELECT alert_id,text FROM alert_notes WHERE tenant_id=%s AND alert_id=ANY(%s::uuid[]) AND NOT deleted ORDER BY id",
        (tenant, ids),
    ):
        notes[row["alert_id"]].append(row["text"])
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
    items = []
    for identity in identities:
        alert = identity["id"]
        if identity["retired_at"]:
            version = identity["final_version"]
            snapshot = {"id": str(alert), "tenant_id": str(tenant), "deleted": True}
        else:
            row = alerts.get(alert)
            if not row:
                raise unavailable("search_authority_gap")
            version = row["projection_version"]
            snapshot = dict(
                wire(row),
                live=True,
                last_received_at=wire(row["timestamp"]),
                notes=notes[alert],
                incident_ids=links[alert],
                active_incident_id=owners.get(alert),
            )
        items.append(
            Projection(
                str(tenant), identity["current_projection_seq"], str(alert), version, snapshot
            )
        )
    return items


def publish_page(db, client, physical_index, epoch, items, *, fence=None):
    from .control import check_writer

    if not items:
        return

    def guard(item, document, candidate_epoch):
        with db.transaction() as conn:
            if fence:
                fence(conn)
            else:
                check_writer(conn, physical_index, epoch)
            row = conn.execute(
                "SELECT retired_at,final_version FROM alert_identities WHERE tenant_id=%s AND id=%s",
                (item.tenant_id, item.alert_id),
            ).fetchone()
            if not row or (row["retired_at"] and item.version < row["final_version"]):
                raise unavailable("search_identity_changed")

    def newer(item, document):
        with db.transaction() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            rows = current_page(conn, item.tenant_id, alert_id=item.alert_id)
            return bool(
                rows and rows[0].version >= item.version and build_document(rows[0]) == document
            )

    # Bound each ES request independently even when current notes make a page big.
    published = []
    started = monotonic()
    for item in items:
        ProjectionWriter(client, physical_index, epoch, max_document_bytes=1048576).publish(
            [item], guard=guard, covered_by_newer=newer
        )
        published.append(item)
        if monotonic() - started >= 0.1:
            break
    return published


def reconcile_page(db, client, physical_index, tenant, epoch, *, fence=None, advance_serving=True):
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
        if state["completed_at"]:
            state = conn.execute(
                "UPDATE search_reconciliation SET boundary_seq=(SELECT projection_seq FROM tenant_work_state WHERE tenant_id=%s),cursor=NULL,completed_at=NULL,started_at=clock_timestamp() WHERE index_epoch=%s AND tenant_id=%s RETURNING *",
                (tenant, epoch, tenant),
            ).fetchone()
    with db.transaction() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        items = current_page(conn, tenant, cursor=state["cursor"])
    items = publish_page(db, client, physical_index, epoch, items, fence=fence) or []
    with db.transaction() as conn:
        if fence:
            fence(conn)
        else:
            check_writer(conn, physical_index, epoch)
        current = conn.execute(
            "SELECT * FROM search_reconciliation WHERE index_epoch=%s AND tenant_id=%s FOR UPDATE",
            (epoch, tenant),
        ).fetchone()
        if (current["boundary_seq"], current["cursor"], current["started_at"]) != (
            state["boundary_seq"],
            state["cursor"],
            state["started_at"],
        ):
            return False
        if items:
            conn.execute(
                "UPDATE search_reconciliation SET cursor=%s,last_page_at=clock_timestamp() WHERE index_epoch=%s AND tenant_id=%s",
                (items[-1].alert_id, epoch, tenant),
            )
            return False
        conn.execute(
            "UPDATE search_reconciliation SET completed_at=clock_timestamp() WHERE index_epoch=%s AND tenant_id=%s",
            (epoch, tenant),
        )
        if advance_serving:
            conn.execute(
                "UPDATE tenant_view_state SET searchable_seq=greatest(searchable_seq,%s),verified_at=clock_timestamp(),view_generation=view_generation+1 WHERE tenant_id=%s AND index_epoch=%s",
                (state["boundary_seq"], tenant, epoch),
            )
        return True


def project_alert(db, settings, tenant, alert_id):
    from elasticsearch import Elasticsearch
    from appchi.service_clients import elasticsearch_options
    from .control import writer_lease

    with writer_lease(db, settings) as physical_index:
        with db.transaction() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            epoch = conn.execute(
                "SELECT index_epoch FROM tenant_view_state WHERE tenant_id=%s", (tenant,)
            ).fetchone()["index_epoch"]
            items = current_page(conn, tenant, alert_id=alert_id)
        with Elasticsearch(settings.elasticsearch_url, **elasticsearch_options(settings)) as client:
            publish_page(db, client, physical_index, epoch, items)


def reconciliation_turn(db, settings):
    from elasticsearch import Elasticsearch
    from appchi.service_clients import elasticsearch_options
    from .control import writer_lease

    with writer_lease(db, settings) as physical_index:
        with db.transaction() as conn:
            if not conn.execute(
                "SELECT ready FROM capability_gates WHERE capability='search'"
            ).fetchone()["ready"]:
                raise unavailable("search_not_ready")
            rows = conn.execute("""SELECT v.tenant_id,v.index_epoch FROM tenant_view_state v LEFT JOIN search_reconciliation r ON r.tenant_id=v.tenant_id AND r.index_epoch=v.index_epoch
                WHERE r.completed_at IS NULL OR r.completed_at<=clock_timestamp()-interval '2 seconds'
                ORDER BY r.last_page_at NULLS FIRST,v.tenant_id LIMIT 1""").fetchall()
        with Elasticsearch(settings.elasticsearch_url, **elasticsearch_options(settings)) as client:
            for row in rows:
                reconcile_page(db, client, physical_index, row["tenant_id"], row["index_epoch"])
