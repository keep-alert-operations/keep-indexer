"""The indexer needs an keep-domain consumer that accepts grouped hint batches."""

import inspect

import pytest

from keep import kafka_changed
from keep.kafka_changed import ChangedConsumer
from keep.search import main
from keep.search.reconciliation import BULK_DOCUMENTS


def test_pinned_domain_accepts_grouped_batches_up_to_the_bulk_limit():
    assert "groups_handler" in inspect.signature(ChangedConsumer).parameters
    assert kafka_changed.MAX_BATCH >= BULK_DOCUMENTS
    main.require_grouped_consumer()


def test_an_older_domain_stops_the_worker_before_the_optional_transport(monkeypatch):
    def old(self, db, settings, profile, role, handler, *, batch_handler=None, batch_size=50):
        pass

    run = []
    monkeypatch.setattr(main, "run", lambda *args, **kwargs: run.append(args))
    monkeypatch.setattr(main.ChangedConsumer, "__init__", old)
    with pytest.raises(SystemExit, match="groups_handler"):
        main.main()
    monkeypatch.undo()
    monkeypatch.setattr(main, "run", lambda *args, **kwargs: run.append(args))
    monkeypatch.delattr(kafka_changed, "MAX_BATCH", raising=False)
    with pytest.raises(SystemExit, match="MAX_BATCH"):
        main.main()
    assert run == []
