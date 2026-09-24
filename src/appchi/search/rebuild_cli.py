"""Operator-facing rebuild CLI; no public user bypass or automatic gate enablement."""

import argparse
import json
from pathlib import Path
import threading

from elasticsearch import Elasticsearch

from appchi.config import Settings
from appchi.db import Database
from appchi.search.rebuild import (
    RebuildBinding,
    bind_serving_index,
    start_rebuild,
    rebuild_turn,
    cancel_rebuild,
)
from appchi.service_clients import elasticsearch_options
from appchi.shared.errors import AppError
from appchi.shared.transactions import wire
from appchi.worker_health import WorkerHealth


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    bind = commands.add_parser("bind")
    bind.add_argument("--epoch", required=True)
    commands.add_parser("start")
    status = commands.add_parser("status")
    status.add_argument("job")
    cancel = commands.add_parser("cancel")
    cancel.add_argument("job")
    resume = commands.add_parser("resume")
    resume.add_argument("job")
    resume.add_argument("--once", action="store_true")
    args = parser.parse_args()
    db = None
    try:
        settings = Settings()
        binding = RebuildBinding.model_validate_json(args.binding.read_text(encoding="utf-8"))
        if binding.alias != settings.elasticsearch_index or not settings.elasticsearch_url:
            parser.error(
                "The binding alias must match APPCHI_ELASTICSEARCH_INDEX and a URL is required"
            )
        db = Database(
            settings.database_url.get_secret_value(),
            settings.pool_size,
            deployment_epoch=settings.deployment_epoch,
        )
        if args.command == "status":
            with db.transaction() as conn:
                job = conn.execute(
                    "SELECT id,state,source_epoch,target_epoch,hold_active,error_code,created_at,updated_at FROM search_rebuild_jobs WHERE id=%s",
                    (args.job,),
                ).fetchone()
            print(json.dumps({"data": wire(job)}))
            return
        with Elasticsearch(settings.elasticsearch_url, **elasticsearch_options(settings)) as client:
            if args.command == "bind":
                bind_serving_index(db, client, binding, args.epoch)
                print(json.dumps({"state": "bound"}))
            elif args.command == "start":
                print(json.dumps({"job_id": start_rebuild(db, binding)}))
            elif args.command == "cancel":
                print(json.dumps({"state": cancel_rebuild(db, client, binding, args.job)}))
            else:
                health = WorkerHealth(settings.worker_health_path, "search-rebuild")
                health.publish()
                stopped = threading.Event()
                try:
                    while True:
                        state = rebuild_turn(db, client, binding, args.job)
                        health.publish(ready=True)
                        print(json.dumps({"job_id": args.job, "state": state}), flush=True)
                        if state in ("complete", "cancelled") or args.once:
                            break
                        health.wait(stopped, 1)
                finally:
                    health.publish(draining=True)
    except (Exception, KeyboardInterrupt) as exc:
        print(
            json.dumps(
                {"error": exc.code if isinstance(exc, AppError) else "search_rebuild_unavailable"}
            )
        )
        raise SystemExit(1) from None
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    main()
