"""Search indexer process entrypoint; PG reconciliation survives Kafka outages."""

import inspect

from keep import kafka_changed
from keep.kafka_changed import ChangedConsumer
from keep.pipeline_runtime import run
from keep.search.consumer import index_record, index_batch, index_groups
from keep.search.reconciliation import BULK_DOCUMENTS, ReconciliationRunner


def require_grouped_consumer():
    """Refuse to start against an keep-domain without grouped hint batches.

    The transport is optional, so a consumer that rejects groups_handler would
    otherwise fail inside the worker loop and leave Kafka hints silently off.
    """
    if (
        "groups_handler" not in inspect.signature(ChangedConsumer).parameters
        or getattr(kafka_changed, "MAX_BATCH", 0) < BULK_DOCUMENTS
    ):
        raise SystemExit(
            "keep-indexer requires keep-domain with ChangedConsumer(groups_handler=...)"
            f" and MAX_BATCH >= {BULK_DOCUMENTS}; update the keep-domain dependency pin"
        )


def main():
    require_grouped_consumer()
    run(
        "indexer",
        lambda db, settings, profile: ChangedConsumer(
            db,
            settings,
            profile,
            "indexer",
            index_record,
            batch_handler=index_batch,
            # One bulk carries every busy partition's hints, up to the writer's
            # 500-document limit, behind a single refresh wait.
            groups_handler=index_groups,
            batch_size=BULK_DOCUMENTS,
        ),
        before_turn=ReconciliationRunner(),
        transport_optional=True,
    )


if __name__ == "__main__":
    main()
