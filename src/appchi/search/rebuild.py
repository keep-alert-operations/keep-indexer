"""Owner-bound Elasticsearch reconstruction from protected immutable PG snapshots.

Each call performs one bounded page or one durable state transition. External
operations never run under a SQL transaction; retries reconcile uncertain writes.
"""

from hashlib import sha256
from typing import Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator

from appchi.models import StrictModel
from appchi.search.projection import (
    Projection,
    build_document,
    canonical,
    index_mapping,
    assert_complete,
)
from appchi.search.writer import ProjectionWriter
from appchi.shared.errors import AppError, unavailable

HOLD_LOCK = 713411725
PAGE_SIZE = 100


class RebuildBinding(StrictModel):
    profile: Literal["appchi-es-rebuild-v1"] = "appchi-es-rebuild-v1"
    alias: str = Field(min_length=1, max_length=200)
    index_prefix: str = Field(min_length=1, max_length=160)
    capability_evidence: str = Field(min_length=1, max_length=1000)
    atomic_alias_switch: Literal[True]
    exclusive_alias_writer: Literal[True]
    max_document_bytes: int = Field(ge=1024, le=104857600)
    index_settings: dict

    @field_validator("alias", "index_prefix")
    @classmethod
    def names(cls, value):
        import re

        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", value):
            raise ValueError("Use an exact lowercase application index name, without wildcards")
        return value

    def digest(self):
        return sha256(canonical(self.model_dump())).hexdigest()


def alias_target(client, alias):
    try:
        aliases = client.indices.get_alias(name=alias)
    except Exception as exc:
        raise unavailable("search_alias_unverified") from exc
    if len(aliases) != 1:
        raise unavailable("search_alias_ambiguous")
    name, value = next(iter(aliases.items()))
    config = value.get("aliases", {}).get(alias, {})
    if (
        any(key in config for key in ("filter", "routing", "index_routing", "search_routing"))
        or config.get("is_write_index") is False
    ):
        raise unavailable("search_alias_scope_mismatch")
    return name


def bind_serving_index(db, client, binding, epoch):
    """Explicit initial binding; never silently adopt an unexpected alias target."""
    UUID(epoch)
    physical = alias_target(client, binding.alias)
    if not physical.startswith(binding.index_prefix):
        raise unavailable("search_index_outside_binding")
    mapping = client.indices.get_mapping(index=physical)[physical]["mappings"]
    expected = index_mapping()
    if (
        mapping.get("_meta") != expected["_meta"]
        or mapping.get("properties") != expected["properties"]
        or mapping.get("dynamic") != "strict"
    ):
        raise unavailable("search_mapping_mismatch")
    with db.transaction() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (HOLD_LOCK,))
        previous = conn.execute(
            "SELECT * FROM search_index_control WHERE singleton FOR UPDATE"
        ).fetchone()
        if previous:
            if (
                previous["alias_name"],
                previous["physical_index"],
                previous["index_epoch"],
                previous["binding_digest"],
            ) != (binding.alias, physical, epoch, binding.digest()):
                raise unavailable("search_binding_conflict")
            return
        conn.execute(
            "INSERT INTO search_index_control(alias_name,physical_index,index_epoch,binding_digest) VALUES(%s,%s,%s,%s)",
            (binding.alias, physical, epoch, binding.digest()),
        )
        # Initial bind is a repair boundary: preexisting visibility must be
        # re-established by current physical-index writers before being claimed.
        conn.execute(
            "UPDATE tenant_view_state SET index_epoch=%s,searchable_seq=0,verified_at=NULL,view_generation=view_generation+1",
            (epoch,),
        )


def start_rebuild(db, binding):
    identity, epoch = uuid4(), str(uuid4())
    with db.transaction() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (HOLD_LOCK,))
        control = conn.execute(
            "SELECT * FROM search_index_control WHERE singleton FOR UPDATE"
        ).fetchone()
        if (
            not control
            or control["binding_digest"] != binding.digest()
            or control["alias_name"] != binding.alias
        ):
            raise unavailable("search_binding_required")
        if control["active_rebuild_id"] or control["writers_paused"]:
            raise AppError("rebuild_in_progress", "Reconcile the existing rebuild first.", 409)
        conn.execute(
            "INSERT INTO search_rebuild_jobs(id,source_index,target_index,source_epoch,target_epoch,binding_digest,state) VALUES(%s,%s,%s,%s,%s,%s,'registered')",
            (
                identity,
                control["physical_index"],
                binding.index_prefix + str(identity),
                control["index_epoch"],
                epoch,
                binding.digest(),
            ),
        )
        conn.execute(
            "UPDATE search_index_control SET active_rebuild_id=%s WHERE singleton", (identity,)
        )
    return str(identity)


def _lease(db, identity, binding):
    token = uuid4()
    with db.transaction() as conn:
        job = conn.execute(
            "SELECT * FROM search_rebuild_jobs WHERE id=%s FOR UPDATE", (identity,)
        ).fetchone()
        if not job or job["binding_digest"] != binding.digest():
            raise unavailable("search_rebuild_binding_mismatch")
        if job["state"] in ("complete", "cancelled"):
            return job, None
        row = conn.execute(
            "UPDATE search_rebuild_jobs SET lease_token=%s,lease_until=clock_timestamp()+interval '30 seconds',updated_at=clock_timestamp() WHERE id=%s AND (lease_until IS NULL OR lease_until<clock_timestamp()) RETURNING *",
            (token, identity),
        ).fetchone()
        if not row:
            raise unavailable("search_rebuild_busy")
        return row, token


def _fence(conn, job, token):
    current = conn.execute(
        "SELECT * FROM search_rebuild_jobs WHERE id=%s AND lease_token=%s AND lease_until>clock_timestamp() FOR UPDATE",
        (job["id"], token),
    ).fetchone()
    if not current or not current["hold_active"] or current["state"] != job["state"]:
        raise unavailable("search_rebuild_fenced")
    return current


def _ensure_index(client, job, binding):
    mapping = index_mapping()
    mapping["_meta"].update(rebuild_id=str(job["id"]), index_epoch=job["target_epoch"])
    if not client.indices.exists(index=job["target_index"]):
        client.indices.create(
            index=job["target_index"], mappings=mapping, settings=binding.index_settings
        )
    observed = client.indices.get_mapping(index=job["target_index"])[job["target_index"]][
        "mappings"
    ]
    if (
        observed.get("_meta") != mapping["_meta"]
        or observed.get("properties") != mapping["properties"]
        or observed.get("dynamic") != "strict"
    ):
        raise unavailable("search_rebuild_index_conflict")


def _capture(db, job, token):
    # Hold registration committed before this consistent snapshot. The immutable
    # projection log remains protected after the short snapshot transaction ends.
    with db.transaction() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        _fence(conn, job, token)
        missing = conn.execute("""SELECT 1 FROM alert_identities i LEFT JOIN alerts a ON a.tenant_id=i.tenant_id AND a.id=i.id
          WHERE NOT EXISTS (SELECT 1 FROM projection_work p WHERE p.tenant_id=i.tenant_id AND p.alert_id=i.id
            AND p.version=CASE WHEN i.retired_at IS NULL THEN a.projection_version ELSE i.final_version END)
          LIMIT 1""").fetchone()
        if missing:
            raise unavailable("search_rebuild_snapshot_gap")
        conn.execute(
            "INSERT INTO search_rebuild_positions(job_id,tenant_id,snapshot_seq) SELECT %s,tenant_id,projection_seq FROM tenant_work_state",
            (job["id"],),
        )
        conn.execute("UPDATE search_rebuild_jobs SET state='copying' WHERE id=%s", (job["id"],))


def _publish(db, client, binding, job, token, rows):
    if not rows:
        return
    items = [
        Projection(
            str(row["tenant_id"]), row["seq"], str(row["alert_id"]), row["version"], row["snapshot"]
        )
        for row in rows
    ]

    def guard(item, document, epoch):
        with db.transaction() as conn:
            _fence(conn, job, token)
            durable = conn.execute(
                "SELECT version,snapshot FROM projection_work WHERE tenant_id=%s AND seq=%s",
                (item.tenant_id, item.seq),
            ).fetchone()
            if (
                not durable
                or durable["version"] != item.version
                or durable["snapshot"] != item.snapshot
            ):
                raise unavailable("search_rebuild_snapshot_gap")

    def newer(item, document):
        with db.transaction() as conn:
            _fence(conn, job, token)
            row = conn.execute(
                "SELECT * FROM projection_work WHERE tenant_id=%s AND alert_id=%s AND version=%s",
                (item.tenant_id, item.alert_id, document.get("projection_version")),
            ).fetchone()
            return bool(
                row
                and build_document(
                    Projection(
                        item.tenant_id, row["seq"], item.alert_id, row["version"], row["snapshot"]
                    )
                )
                == document
            )

    ProjectionWriter(
        client,
        job["target_index"],
        job["target_epoch"],
        max_document_bytes=binding.max_document_bytes,
    ).publish(items, guard=guard, covered_by_newer=newer)


def _byte_page(rows):
    size, selected = 0, []
    for row in rows:
        item = Projection(
            str(row["tenant_id"]), row["seq"], str(row["alert_id"]), row["version"], row["snapshot"]
        )
        length = len(canonical(build_document(item))) + 512
        if selected and size + length > 3 * 1024 * 1024:
            break
        selected.append(row)
        size += length
    return selected


def _copy(db, client, binding, job, token):
    with db.transaction() as conn:
        _fence(conn, job, token)
        pos = conn.execute(
            "SELECT * FROM search_rebuild_positions WHERE job_id=%s AND NOT copy_done ORDER BY tenant_id LIMIT 1",
            (job["id"],),
        ).fetchone()
        if not pos:
            conn.execute(
                "UPDATE search_rebuild_jobs SET state='catching_up' WHERE id=%s", (job["id"],)
            )
            return
        rows = conn.execute(
            "SELECT DISTINCT ON (alert_id) * FROM projection_work WHERE tenant_id=%s AND seq<=%s AND (%s::uuid IS NULL OR alert_id>%s::uuid) ORDER BY alert_id,seq DESC LIMIT %s",
            (
                pos["tenant_id"],
                pos["snapshot_seq"],
                pos["copy_cursor"],
                pos["copy_cursor"],
                PAGE_SIZE,
            ),
        ).fetchall()
    rows = _byte_page(rows)
    _publish(db, client, binding, job, token, rows)
    with db.transaction() as conn:
        _fence(conn, job, token)
        if not rows:
            conn.execute(
                "UPDATE search_rebuild_positions SET copy_done=true,copied_seq=snapshot_seq WHERE job_id=%s AND tenant_id=%s",
                (job["id"], pos["tenant_id"]),
            )
        else:
            conn.execute(
                "UPDATE search_rebuild_positions SET copy_cursor=%s WHERE job_id=%s AND tenant_id=%s",
                (rows[-1]["alert_id"], job["id"], pos["tenant_id"]),
            )


def _catch_up(db, client, binding, job, token):
    with db.transaction() as conn:
        _fence(conn, job, token)
        if job["state"] == "catching_up":
            conn.execute(
                "INSERT INTO search_rebuild_positions(job_id,tenant_id,snapshot_seq,copy_done) SELECT %s,tenant_id,0,true FROM tenant_work_state ON CONFLICT DO NOTHING",
                (job["id"],),
            )
        pos = conn.execute(
            "SELECT p.*,coalesce(p.cutover_seq,w.projection_seq) AS bound FROM search_rebuild_positions p JOIN tenant_work_state w USING(tenant_id) WHERE p.job_id=%s AND p.copied_seq<coalesce(p.cutover_seq,w.projection_seq) ORDER BY p.tenant_id LIMIT 1",
            (job["id"],),
        ).fetchone()
        rows = (
            []
            if not pos
            else conn.execute(
                "SELECT * FROM projection_work WHERE tenant_id=%s AND seq>%s AND seq<=%s ORDER BY seq LIMIT %s",
                (pos["tenant_id"], pos["copied_seq"], pos["bound"], PAGE_SIZE),
            ).fetchall()
        )
        if pos and (
            not rows
            or any(row["seq"] != pos["copied_seq"] + index + 1 for index, row in enumerate(rows))
        ):
            raise unavailable("search_rebuild_snapshot_gap")
    if pos:
        rows = _byte_page(rows)
        _publish(db, client, binding, job, token, rows)
        with db.transaction() as conn:
            _fence(conn, job, token)
            conn.execute(
                "UPDATE search_rebuild_positions SET copied_seq=%s WHERE job_id=%s AND tenant_id=%s",
                (rows[-1]["seq"], job["id"], pos["tenant_id"]),
            )
        return
    with db.transaction() as conn:
        # Control precedes job and tenant locks, matching projection writers.
        conn.execute(
            "SELECT singleton FROM search_index_control WHERE singleton FOR UPDATE"
        ).fetchone()
        _fence(conn, job, token)
        if job["state"] == "catching_up":
            conn.execute("UPDATE search_index_control SET writers_paused=true WHERE singleton")
            conn.execute(
                "UPDATE search_rebuild_jobs SET state='draining' WHERE id=%s", (job["id"],)
            )
        else:
            conn.execute(
                "UPDATE search_rebuild_jobs SET state='validating' WHERE id=%s", (job["id"],)
            )


def _drain(db, job, token):
    with db.transaction() as conn:
        conn.execute(
            "SELECT singleton FROM search_index_control WHERE singleton FOR UPDATE"
        ).fetchone()
        _fence(conn, job, token)
        conn.execute("DELETE FROM search_writer_leases WHERE expires_at<=clock_timestamp()")
        if conn.execute("SELECT 1 FROM search_writer_leases LIMIT 1").fetchone():
            return
        # One statement captures all tenant positions consistently; later writes
        # stay queued, including tenants created after this cutover snapshot.
        conn.execute(
            "INSERT INTO search_rebuild_positions(job_id,tenant_id,snapshot_seq,copy_done,cutover_seq) SELECT %s,tenant_id,0,true,projection_seq FROM tenant_work_state ON CONFLICT(job_id,tenant_id) DO UPDATE SET cutover_seq=excluded.cutover_seq",
            (job["id"],),
        )
        conn.execute("UPDATE search_rebuild_jobs SET state='validating' WHERE id=%s", (job["id"],))


def _validate(db, client, binding, job, token):
    with db.transaction() as conn:
        _fence(conn, job, token)
        pending = conn.execute(
            "SELECT 1 FROM search_rebuild_positions WHERE job_id=%s AND copied_seq<cutover_seq LIMIT 1",
            (job["id"],),
        ).fetchone()
    if pending:
        _catch_up(db, client, binding, job, token)
        return
    _ensure_index(client, job, binding)
    arguments = dict(
        index=job["target_index"],
        query={"match_all": {}},
        sort=[{"id": "asc"}],
        size=PAGE_SIZE,
        track_total_hits=True,
        version=True,
        allow_partial_search_results=False,
    )
    if job["validation_cursor"]:
        arguments["search_after"] = [job["validation_cursor"]]
    result = client.search(**arguments)
    assert_complete(result)
    hits = result["hits"]["hits"]
    if result["hits"]["total"]["relation"] != "eq":
        raise unavailable("search_rebuild_validation_incomplete")
    with db.transaction() as conn:
        _fence(conn, job, token)
        for hit in hits:
            document = hit["_source"]
            try:
                tenant, identity = UUID(document["tenant_id"]), UUID(document["id"])
            except (ValueError, KeyError) as exc:
                raise unavailable("search_rebuild_scope_mismatch") from exc
            row = conn.execute(
                "SELECT w.* FROM projection_work w JOIN search_rebuild_positions p ON p.tenant_id=w.tenant_id AND p.job_id=%s WHERE w.tenant_id=%s AND w.alert_id=%s AND w.seq<=p.cutover_seq ORDER BY w.seq DESC LIMIT 1",
                (job["id"], tenant, identity),
            ).fetchone()
            if (
                not row
                or hit["_id"] != str(identity)
                or hit["_version"] != row["version"]
                or document
                != build_document(
                    Projection(
                        str(tenant), row["seq"], str(identity), row["version"], row["snapshot"]
                    )
                )
            ):
                raise unavailable("search_rebuild_parity_mismatch")
        count = job["validation_count"] + len(hits)
        if len(hits) == PAGE_SIZE:
            conn.execute(
                "UPDATE search_rebuild_jobs SET validation_count=%s,validation_cursor=%s WHERE id=%s",
                (count, hits[-1]["_source"]["id"], job["id"]),
            )
            return
        expected = conn.execute(
            "SELECT count(*) AS n FROM (SELECT DISTINCT w.tenant_id,w.alert_id FROM projection_work w JOIN search_rebuild_positions p ON p.tenant_id=w.tenant_id AND p.job_id=%s WHERE w.seq<=p.cutover_seq) expected",
            (job["id"],),
        ).fetchone()["n"]
        if expected != count or count != result["hits"]["total"]["value"]:
            raise unavailable("search_rebuild_count_mismatch")
        conn.execute(
            "UPDATE search_rebuild_jobs SET state='switching',validation_count=%s WHERE id=%s",
            (count, job["id"]),
        )


def _switch(db, client, binding, job, token):
    with db.transaction() as conn:
        _fence(conn, job, token)
    target = alias_target(client, binding.alias)
    if target == job["source_index"]:
        # must_exist makes an unexpected missing old binding fail the whole list.
        client.indices.update_aliases(
            actions=[
                {
                    "remove": {
                        "index": job["source_index"],
                        "alias": binding.alias,
                        "must_exist": True,
                    }
                },
                {
                    "add": {
                        "index": job["target_index"],
                        "alias": binding.alias,
                        "is_write_index": True,
                    }
                },
            ]
        )
        target = alias_target(client, binding.alias)
    if target != job["target_index"]:
        raise unavailable("search_alias_ambiguous")
    with db.transaction() as conn:
        conn.execute(
            "SELECT singleton FROM search_index_control WHERE singleton FOR UPDATE"
        ).fetchone()
        _fence(conn, job, token)
        conn.execute(
            "UPDATE tenant_view_state v SET index_epoch=%s,searchable_seq=coalesce((SELECT p.cutover_seq FROM search_rebuild_positions p WHERE p.job_id=%s AND p.tenant_id=v.tenant_id),0),verified_at=clock_timestamp(),view_generation=view_generation+1",
            (job["target_epoch"], job["id"]),
        )
        conn.execute(
            "UPDATE search_index_control SET physical_index=%s,index_epoch=%s,writers_paused=false,active_rebuild_id=NULL,updated_at=clock_timestamp() WHERE singleton",
            (job["target_index"], job["target_epoch"]),
        )
        conn.execute(
            "UPDATE search_rebuild_jobs SET state='complete',hold_active=false,updated_at=clock_timestamp() WHERE id=%s",
            (job["id"],),
        )


def rebuild_turn(db, client, binding, identity):
    job, token = _lease(db, identity, binding)
    if token is None:
        return job["state"]
    try:
        if job["state"] == "registered":
            _ensure_index(client, job, binding)
            _capture(db, job, token)
        elif job["state"] == "copying":
            _copy(db, client, binding, job, token)
        elif job["state"] == "catching_up":
            _catch_up(db, client, binding, job, token)
        elif job["state"] == "draining":
            _drain(db, job, token)
        elif job["state"] == "validating":
            _validate(db, client, binding, job, token)
        elif job["state"] == "switching":
            _switch(db, client, binding, job, token)
    except Exception as exc:
        with db.transaction() as conn:
            conn.execute(
                "UPDATE search_rebuild_jobs SET error_code=%s WHERE id=%s AND lease_token=%s",
                (
                    exc.code if isinstance(exc, AppError) else "search_rebuild_dependency",
                    job["id"],
                    token,
                ),
            )
        raise
    finally:
        with db.transaction() as conn:
            conn.execute(
                "UPDATE search_rebuild_jobs SET lease_token=NULL,lease_until=NULL WHERE id=%s AND lease_token=%s",
                (job["id"], token),
            )
    with db.transaction() as conn:
        return conn.execute(
            "SELECT state FROM search_rebuild_jobs WHERE id=%s", (job["id"],)
        ).fetchone()["state"]


def cancel_rebuild(db, client, binding, identity):
    job, token = _lease(db, identity, binding)
    if token is None:
        return job["state"]
    try:
        if job["state"] == "switching":
            raise AppError(
                "search_switch_requires_reconciliation",
                "Resume the uncertain alias switch before any further rebuild.",
                409,
            )
        if alias_target(client, binding.alias) != job["source_index"]:
            raise unavailable("search_alias_ambiguous")
        with db.transaction() as conn:
            conn.execute(
                "SELECT singleton FROM search_index_control WHERE singleton FOR UPDATE"
            ).fetchone()
            _fence(conn, job, token)
            conn.execute(
                "UPDATE search_rebuild_jobs SET state='cancelled',hold_active=false,updated_at=clock_timestamp() WHERE id=%s",
                (job["id"],),
            )
            conn.execute(
                "UPDATE search_index_control SET writers_paused=false,active_rebuild_id=NULL WHERE singleton"
            )
        return "cancelled"
    finally:
        with db.transaction() as conn:
            conn.execute(
                "UPDATE search_rebuild_jobs SET lease_token=NULL,lease_until=NULL WHERE id=%s AND lease_token=%s",
                (job["id"], token),
            )
