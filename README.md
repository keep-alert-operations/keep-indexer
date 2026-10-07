# appchi-indexer

Current-alert Elasticsearch projection, PostgreSQL reconciliation and index rebuild operations.

See [system design](https://github.com/appchi-alert-operations/appchi-design), [workspace setup](https://github.com/appchi-alert-operations/appchi-delivery) and [source provenance](SOURCE.md).

## Development

Use Python 3.13 and the committed `uv.lock`. Configure Git authentication for the
private organization repositories before resolving internal dependencies.

```powershell
uv sync --locked --all-extras
uv run --locked ruff check src tests
uv run --locked pytest
uv build --wheel
```

Shared `appchi` modules use namespace packaging; each module has exactly one owning
distribution. Dependencies resolve immutable Git revisions in Python dependency metadata and `uv.lock`.
For coordinated local edits use the delivery workspace setup, which installs local
packages without changing release dependency pins.

## Execution

`uv run --locked appchi-indexer` is the entrypoint. Supply the documented environment
bindings; package installation does not start services or migrate data.

## Projection capacity and scan ownership

Apply migration `0049_search_reconciliation_leases.sql` before this indexer runs.
Migration `0050_alert_notes_projection_lookup.sql` supplies the partial live-note
lookup index used by projection pages. Pages probe for live notes with the alert
query and open bounded note streams only for alerts that have them.
The process reuses one Elasticsearch client for Kafka hints and authority scans.
It publishes at most 50 current documents in a normal 4 MiB bulk, verifies one
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

Kafka handlers commit only a successfully covered contiguous partition prefix.
A failed input pauses that partition for bounded retry at the same offset; other
partitions continue. Retention loss on source/correlation is a persistent partition
fence requiring explicit repair and restart, with no automatic offset skip.
