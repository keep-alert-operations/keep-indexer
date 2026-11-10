# keep-indexer

Current-alert Elasticsearch projection, PostgreSQL reconciliation and index rebuild operations.

See [system design](https://github.com/keep-alert-operations/keep-design), [workspace setup](https://github.com/keep-alert-operations/keep-delivery) and [source provenance](SOURCE.md).

## Development

Use Python 3.13, Poetry 2.3.1 and the committed `poetry.lock`. Configure Git authentication for the
private organization repositories before resolving internal dependencies.

```powershell
poetry check --lock
poetry sync --all-extras
poetry run ruff check src tests
poetry run pytest
poetry build --format wheel
```

Shared `keep` modules use namespace packaging; each module has exactly one owning
distribution. Dependencies resolve immutable Git revisions in Python dependency metadata and `poetry.lock`.
For coordinated local edits use the delivery workspace setup, which installs local
packages without changing release dependency pins.

## Execution

`poetry run keep-indexer` is the entrypoint. Supply the documented environment
bindings; package installation does not start services or migrate data.

## Projection capacity and scan ownership

Apply migration `0049_search_reconciliation_leases.sql` before this indexer runs.
Migration `0050_alert_notes_projection_lookup.sql` supplies the partial live-note
lookup index used by projection pages. Pages probe for live notes with the alert
query and open bounded note streams only for alerts that have them.
The process reuses one Elasticsearch client for Kafka hints and authority scans.
Authority scans publish pages of at most 50 current documents. Kafka hints from
every busy partition share one bulk of at most 500 documents and 4 MiB; each
partition commits only its own covered prefix. After a failed shared request,
every non-head partition it included is left out of shared requests until it has
handled every input it had in that request alone (or was fenced, revoked or lost), and no
shared request is made while such a partition is the head. Each bulk verifies one
refresh-visible result for the batch, and skips writes for already visible identical
versions. A large individual document can use a separate request within the
explicit document allocation; notes and their searchable tokens are not truncated.

Staging and production capacity profiles must provide a positive integer
`search_max_document_bytes` between 1,024 and 104,857,600. This is an application
allocation to qualify against the supplied Elasticsearch and proxy request limits,
including bulk metadata overhead; it does not discover or change those limits.
Development without this field uses an unqualified 100 MiB diagnostic ceiling.
The rebuild binding's explicit `max_document_bytes` must accommodate the same data.
All finite single-document allocations can eventually be exhausted by an unbounded
note collection; such an alert remains in PostgreSQL and reports an isolated
`projection_document_capacity` failure until its integration allocation or projection
representation is addressed. No data is omitted to make a successful projection.

Scans claim a tenant page for 30 seconds and recheck that ownership before publication
and before advancing progress. Different replicas can claim different tenants.
Failure records a controlled `last_error` and a five-second `retry_after` on that
tenant's `search_reconciliation` row, releases the claim, and leaves its cursor and
searchable watermark unchanged. Healthy tenants and Kafka hints continue. An expired
claim cannot advance progress after another replica has taken ownership. Existing
physical-index writer leases and external-version fences remain in force.

After a completed full pass, ordinary scans select only identities whose current
projection sequence exceeds the prior verified watermark. Each pass keeps its
captured floor and completion boundary fixed; a later update behind its UUID
cursor is covered by the next pass. Unchanged retained notes are not repeatedly
tokenized. The first pass for an epoch scans every identity. A separate audit is
due five minutes after the last full pass: its own cursor scans a captured finite
sequence generation, interleaving at most one audit page per four ordinary turns.
Newer writes remain the ordinary delta scan's responsibility. Audit pages never
advance or block the ordinary watermark. This repairs externally missing unchanged
documents within five minutes plus scheduling and full-audit time; that repair
bound is separate from normal alert search freshness targets. Foreign higher
external versions still require operational repair. Rebuilds continue using
complete authority scans.

Audit failures use separate `audit_retry_after` and `audit_last_error` fields;
ordinary delta scans continue, and their success does not clear an unresolved
audit error. Audit ownership is rechecked after locking its progress row before
starting or completing work.
Completed and empty ordinary passes update their scheduling timestamp so idle
tenants yield to pending tenants instead of retaining permanent scan priority.

Before taking a physical-index writer lease, a scan reads in one statement
whether the search gate is open and when the next tenant becomes claimable (the
same predicate as the claim: unfinished or audit passes, two seconds after a
completed pass, claim expiry and `retry_after`). While nothing is due, the
worker issues no statement until that time, rechecking at least once a second so
new tenants, index epochs and gate changes are seen; it takes no lease, inserts
no reconciliation row and runs no claim transaction. Due work still takes the
writer lease, so paused or rebound writers stay fenced on every turn. Kafka hints
are unaffected: the consumer's bounded poll keeps running each loop. Waiting
until the scan's next due time (at most one second) during a Kafka outage needs
the keep-domain worker loop revision from keep-alert-operations/keep-domain#10;
until this repository pins it, an outage retries the transport every 10
milliseconds, but the idle scan itself still issues no statement until due.

Kafka handlers commit only a successfully covered contiguous partition prefix.
A failed input pauses that partition for bounded retry at the same offset; other
partitions continue. Retention loss on source/correlation is a persistent partition
fence requiring explicit repair and restart, with no automatic offset skip.

## Initial search activation for an isolated development cluster

The explicit `keep.search.bootstrap` command enables a **fresh development
search projection** for diagnostics such as a private OpenShift load test. It is
not a restore command or production qualification. The existing
`keep.search.local_bootstrap` remains restricted to its fixed loopback database
and alias; none of its isolation checks are relaxed.

Run the dedicated PostgreSQL migration job first. Keep ingestion and all writers
stopped until this command succeeds. Use the normal `keep_worker` database
identity with the migration-granted permissions, not a database administrator.
Do not grant extra restore privileges. This command never runs migrations.

Supply the regular indexer `Settings` environment, including:

| Setting | Requirement |
| --- | --- |
| `KEEP_ENVIRONMENT` | Exactly `development`. |
| `KEEP_ROLE` | Exactly `indexer`. |
| `KEEP_DEPLOYMENT_EPOCH` | Same deployment epoch as the cluster's runtime services. |
| `KEEP_DATABASE_URL` | The isolated, migrated Keep database using the worker identity and the required PostgreSQL TLS parameters. |
| `KEEP_WRAPPING_KEY` or `KEEP_WRAPPING_KEYRING` | The existing valid runtime key configuration required by `Settings`. Do not generate a different key for each retry. |
| `KEEP_ELASTICSEARCH_URL` | HTTPS root origin, without user information, path prefix, query or fragment. |
| `KEEP_ELASTICSEARCH_INDEX` | Exact serving alias from the binding below. |
| `KEEP_ELASTICSEARCH_API_KEY` | Target-scoped credential, if required by supplied Elasticsearch. |
| `KEEP_ELASTICSEARCH_CA_FILE` | Mounted private CA bundle when the target uses a private issuer. TLS verification remains enabled. |
| `KEEP_ELASTICSEARCH_CLIENT_CERT` / `KEEP_ELASTICSEARCH_CLIENT_KEY` | Existing optional mutual-TLS settings, supplied together. |
| `KEEP_CLOCK_HEALTH_FILE` / `KEEP_DATABASE_CLOCK_HEALTH_FILE` | Both owner-published clock evidence files, using the ordinary runtime format. They must remain fresh through final activation. |

Clock evidence is checked by the existing clock validator: synchronized `true`,
finite offset within one second, and `measured_at` no more than 60 seconds old.
Provide refreshed observations; do not turn an old observation into fresh evidence
by replacing its timestamp.

Use a validated `RebuildBinding` JSON file. This example uses illustrative names
and a single-shard, no-replica diagnostic allocation; choose an allocation agreed
with the Elasticsearch owner rather than treating these values as production
sizing:

```json
{
  "profile": "keep-es-rebuild-v1",
  "alias": "keep-loadtest-alerts",
  "index_prefix": "keep-loadtest-alerts-",
  "capability_evidence": "YOUR-SUPPLIED-INTEGRATION-REFERENCE",
  "atomic_alias_switch": true,
  "exclusive_alias_writer": true,
  "max_document_bytes": 1048576,
  "index_settings": {"number_of_shards": 1, "number_of_replicas": 0}
}
```

The two boolean fields retain their existing binding contract: the supplied
integration must support atomic alias switching and designate one alias writer.
The command does not infer operational exclusivity from a successful probe.
`capability_evidence` is the supplied integration reference, not a JSON assertion
that tests passed. Initial bootstrap accepts only explicit integer
`number_of_shards` (positive) and `number_of_replicas` (nonnegative) settings.
`max_document_bytes` must accommodate the real synthetic compatibility fixtures;
an insufficient limit fails rather than silently omitting that check.

Choose and retain one physical index name under `index_prefix`, one UUID index
epoch, and one nonblank operation reference (at most 200 characters). For example:

```sh
python -m keep.search.bootstrap \
  --binding /etc/keep/bootstrap/search-binding.json \
  --physical-index keep-loadtest-alerts-initial \
  --index-epoch YOUR-LOWERCASE-UUID \
  --operation-reference YOUR-LOADTEST-RUN-REFERENCE \
  --ingestion-and-writers-stopped
```

The last flag is an operator attestation that producers and writers are stopped;
it does not stop workloads. The command also enforces PostgreSQL concurrency
guards: a shared restore lock spans the operation, the existing search hold lock
serializes it with rebuild operations, and short `NOWAIT` table locks prevent
canonical writes during the final empty-state check and activation. External
Elasticsearch calls never run inside a SQL transaction. An in-flight writer makes
the command fail, leaving an initial search gate closed.

The database must have no alert identities or projection work, zero tenant
admission/change/projection/search watermarks, complete unblocked tenant state,
and no active search writer leases. Unexpected tenant/index epochs, existing
rebuilds, conflicting bindings, fenced or previously restored databases, foreign
indices/aliases, unexpected mappings/settings, and any indexed documents are
rejected. A refresh precedes the exact empty-index check so unrefreshed documents
cannot be silently adopted.

The command reserves its identity durably before creating the serving index.
Exact canonical mappings carry additional `_meta.initial_bootstrap` ownership
metadata, allowing an interrupted operation to identify only its own resources.
It then creates two disposable indices named
`<index_prefix>bootstrap-probe-<index_epoch>-a` and `-b`, plus an `-alias`, and
exercises the real native CEL compiler, projection writer, search reader and
count reader: strict mappings, external versions, refreshed visibility, tenant
isolation, Unicode tokenization, PIT pagination, exact count batches, tombstone
replay and one-request alias switching. Probe cleanup is required before
activation. Elasticsearch credentials need cluster information access and index
create/delete, mapping/settings/alias, write/refresh, search/PIT and alias-management
permissions scoped to the exact serving alias and the selected index prefix.

On success, all current empty tenant search watermarks, the serving control and
the search gate change in one SQL transaction. JSON output and audit records
include actual probe results, target cluster UUID/version, binding/mapping
digests, operation identity, and `production_qualified: false`. This is a bounded
development compatibility diagnostic, not proof of capacity, availability,
exclusive infrastructure ownership or the complete production verification plan.

Retry an interrupted run with the **same binding, endpoint/target, deployment
epoch, physical index, index epoch and operation reference**. Matching owned
resources are checked and reused, and an absent alias can be repaired. Disposable
probe residue is deleted only after exact ownership/mapping checks. A successful
repeat is allowed while the database/index remain empty and returns
`already_active`; every repeat executes fresh probes. Once source data exists,
use the normal indexer/rebuild workflow. Never reset restore gates, falsify audit
records, adopt a foreign index, or erase populated data to make this initializer
pass. Errors return a sanitized code and a nonzero exit status.

### Verification

The ordinary unit suite covers configuration, clock, mapping and lock failure
guards. Opt-in integration tests create/drop **new random databases** and random
Elasticsearch namespaces, exercising actual `keep_worker` privileges, live
probes, crash retry, restore fences and concurrent writes:

```sh
# Inject these privately, never commit their values:
# KEEP_BOOTSTRAP_TEST_POSTGRES_ADMIN_DSN (isolated server, CREATE DATABASE)
# KEEP_TEST_ELASTICSEARCH_URL (isolated HTTPS service)
# KEEP_TEST_ELASTICSEARCH_CA (optional trusted private CA path)
# KEEP_TEST_ELASTICSEARCH_API_KEY (optional isolated test credential)
python -m pytest tests/integration/test_search_bootstrap_integration.py -q
```

The integration environment also needs the separately installed
`keep-migrations` package; the coordinated delivery development environment
provides it. These fixture migrations run only in their freshly created test
databases. They are not part of runtime or bootstrap startup.
