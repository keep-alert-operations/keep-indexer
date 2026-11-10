from uuid import UUID
from keep.event_models import ChangedEnvelope
from keep.ingestion.native import parse_json
from keep.shared.errors import unavailable
from keep.search.reconciliation import service_for


def index_record(db, settings, profile, message):
    envelope = ChangedEnvelope.model_validate(parse_json(message.value()))
    if message.key() != UUID(envelope.tenant_id).bytes:
        raise unavailable("changed_tenant_key_invalid")
    covered = service_for(db, settings, profile).project(
        UUID(envelope.tenant_id), [UUID(envelope.alert_id)]
    )
    if envelope.alert_id not in covered:
        raise unavailable("search_hint_authority_missing")


def index_batch(db, settings, profile, messages):
    """Coalesce current-state hints and acknowledge only a covered prefix."""
    envelopes = [
        ChangedEnvelope.model_validate(parse_json(message.value())) for message in messages
    ]
    for message, envelope in zip(messages, envelopes):
        if message.key() != UUID(envelope.tenant_id).bytes:
            raise unavailable("changed_tenant_key_invalid")
    count = covered_prefixes(db, settings, profile, [envelopes])[0]
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
    return covered_prefixes(db, settings, profile, parsed)


def covered_prefixes(db, settings, profile, groups):
    """Project every group's hints in one request; count each covered prefix."""
    # One partition may carry many tenants; one physical-index bulk can cover
    # all of them while per-document authority and tenant checks stay intact.
    requests = list(
        dict.fromkeys(
            (envelope.tenant_id, envelope.alert_id)
            for envelopes in groups
            for envelope in envelopes
        )
    )
    covered = service_for(db, settings, profile).project_many(requests) if requests else set()
    counts = []
    for envelopes in groups:
        count = 0
        for envelope in envelopes:
            if (envelope.tenant_id, envelope.alert_id) not in covered:
                break
            count += 1
        counts.append(count)
    return counts
