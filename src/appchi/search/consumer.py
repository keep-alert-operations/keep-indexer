from uuid import UUID
from appchi.event_models import ChangedEnvelope
from appchi.ingestion.native import parse_json
from appchi.shared.errors import unavailable
from appchi.search.reconciliation import project_alert


def index_record(db, settings, profile, message):
    envelope = ChangedEnvelope.model_validate(parse_json(message.value()))
    if message.key() != UUID(envelope.tenant_id).bytes:
        raise unavailable("changed_tenant_key_invalid")
    project_alert(db, settings, UUID(envelope.tenant_id), UUID(envelope.alert_id))
