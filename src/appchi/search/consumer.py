from uuid import UUID
from appchi.event_models import ChangedEnvelope
from appchi.ingestion.native import parse_json
from appchi.shared.errors import unavailable
from appchi.search.reconciliation import service_for


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
    # One partition may carry many tenants; one physical-index bulk can cover
    # all of them while per-document authority and tenant checks stay intact.
    requests = list(
        dict.fromkeys((envelope.tenant_id, envelope.alert_id) for envelope in envelopes)
    )
    covered = service_for(db, settings, profile).project_many(requests)
    count = 0
    for envelope in envelopes:
        if (envelope.tenant_id, envelope.alert_id) not in covered:
            break
        count += 1
    if not count:
        raise unavailable("search_hint_authority_missing")
    return count
