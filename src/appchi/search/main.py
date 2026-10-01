"""Search indexer process entrypoint; PG reconciliation survives Kafka outages."""

from appchi.kafka_changed import ChangedConsumer
from appchi.pipeline_runtime import run
from appchi.search.consumer import index_record
from appchi.search.reconciliation import reconciliation_turn


def main():
    run(
        "indexer",
        lambda db, settings, profile: ChangedConsumer(
            db,
            settings,
            profile,
            "indexer",
            index_record,
        ),
        before_turn=reconciliation_turn,
        transport_optional=True,
    )


if __name__ == "__main__":
    main()
