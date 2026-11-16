import logging
from uuid import UUID
from keep.event_models import ChangedEnvelope
from keep.ingestion.native import parse_json
from keep.kafka_changed import changed_flow, seconds_since
from keep.observability import alert_fields, register
from keep.shared.errors import unavailable
from keep.search.reconciliation import service_for

log = logging.getLogger(__name__)

# The steps one changed event takes inside the indexer (observability 3): it is
# read from Kafka (alert_change_consumed), then its alert is confirmed searchable.
register(
    "alert_indexed",
    logging.INFO,
    "Alert is searchable at document version {revision} for projection sequence "
    "{projection_seq}",
    "The search document covering this change is visible in Elasticsearch. "
    "delay_seconds is the time since the change was committed.",
    required=("changed_event_id", "alert"),
    outcome="success",
)
register(
    "alert_index_deferred",
    logging.INFO,
    "Alert is not yet confirmed searchable; the changed event is handled again",
    "A shared bulk request did not reach this change, or its authority was not ready. "
    "The partition handles it again on its own; a lasting failure is then recorded as "
    "changed_partition_retry.",
    required=("changed_event_id", "alert"),
    outcome="unknown",
)


def _flows(messages, envelopes):
    return [changed_flow(log, "indexer", m, e) for m, e in zip(messages, envelopes)]


def _indexed(service, flow, envelope):
    """Record that the document covering one changed event is visible."""
    projected = getattr(service, "projected", None) or {}
    item = projected.get((envelope.tenant_id, envelope.alert_id))
    flow.step(
        "alert_indexed",
        alert=alert_fields(item.snapshot) if item is not None else None,
        revision=item.version if item is not None else None,
        delay_seconds=seconds_since(getattr(envelope, "recorded_at", None)),
        duration=flow.elapsed(),
    )


def index_record(db, settings, profile, message):
    envelope = ChangedEnvelope.model_validate(parse_json(message.value()))
    if message.key() != UUID(envelope.tenant_id).bytes:
        raise unavailable("changed_tenant_key_invalid")
    flow = changed_flow(log, "indexer", message, envelope)
    service = service_for(db, settings, profile)
    covered = service.project(UUID(envelope.tenant_id), [UUID(envelope.alert_id)])
    if envelope.alert_id not in covered:
        raise unavailable("search_hint_authority_missing")
    _indexed(service, flow, envelope)


def index_batch(db, settings, profile, messages):
    """Coalesce current-state hints and acknowledge only a covered prefix."""
    envelopes = [
        ChangedEnvelope.model_validate(parse_json(message.value())) for message in messages
    ]
    for message, envelope in zip(messages, envelopes):
        if message.key() != UUID(envelope.tenant_id).bytes:
            raise unavailable("changed_tenant_key_invalid")
    count = covered_prefixes(
        db, settings, profile, [envelopes], flows=[_flows(messages, envelopes)]
    )[0]
    if not count:
        raise unavailable("search_hint_authority_missing")
    return count


def index_groups(db, settings, profile, groups):
    """Index several partitions' hints through one bulk and one refresh wait.

    Returns each group's covered prefix length. A group stops before its first
    unparsable or wrongly keyed hint, and a group the bulk did not reach (or
    whose hint has no authority) reports 0: the consumer then handles it alone,
    where faults are isolated per partition.
    """
    parsed = []
    for messages in groups:
        envelopes = []
        for message in messages:
            try:
                envelope = ChangedEnvelope.model_validate(parse_json(message.value()))
                valid = message.key() == UUID(envelope.tenant_id).bytes
            except Exception:
                valid = False
            if not valid:
                break
            envelopes.append(envelope)
        parsed.append(envelopes)
    flows = [_flows(messages, envelopes) for messages, envelopes in zip(groups, parsed)]
    return covered_prefixes(db, settings, profile, parsed, flows=flows)


def covered_prefixes(db, settings, profile, groups, *, flows=None):
    """Project every group's hints in one request; count each covered prefix.

    ``flows`` holds the flow of each hint, in the shape of ``groups``; a covered
    hint is recorded as indexed and the first uncovered one as deferred.
    """
    # One partition may carry many tenants; one physical-index bulk can cover
    # all of them while per-document authority and tenant checks stay intact.
    requests = list(
        dict.fromkeys(
            (envelope.tenant_id, envelope.alert_id)
            for envelopes in groups
            for envelope in envelopes
        )
    )
    service = service_for(db, settings, profile)
    covered = service.project_many(requests) if requests else set()
    counts = []
    for position, envelopes in enumerate(groups):
        count = 0
        for envelope in envelopes:
            flow = flows[position][count] if flows else None
            if (envelope.tenant_id, envelope.alert_id) not in covered:
                if flow is not None:
                    flow.step("alert_index_deferred")
                break
            if flow is not None:
                _indexed(service, flow, envelope)
            count += 1
        counts.append(count)
    return counts
