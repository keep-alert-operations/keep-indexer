"""Search indexer process entrypoint; PG reconciliation survives Kafka outages."""

from appchi.kafka_changed import ChangedConsumer
from appchi.pipeline_runtime import run
from appchi.search.consumer import index_record, index_batch
from appchi.search.reconciliation import ReconciliationRunner


def main():
    run(
        "indexer",
        lambda db, settings, profile: ChangedConsumer(
            db,
            settings,
            profile,
            "indexer",
            index_record,
            batch_handler=index_batch,
        ),
        before_turn=ReconciliationRunner(),
        transport_optional=True,
    )


if __name__ == "__main__":
    main()
