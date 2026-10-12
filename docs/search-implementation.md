# Search adapter implementation

`backend/appchi/search` uses the official Elasticsearch Python client's public
bulk, search, PIT and index-mapping APIs. It does not substitute an in-memory
search engine, post-filter all tenant alerts, use scripts or send user query DSL.

## Projection boundary

`index_mapping()` supplies strict mappings with ordinary stored `_source`,
keyword identifiers, a wildcard representation for full-length messages,
signed epoch-microsecond long timestamps, explicit null-last sorting flags,
clear-last severity sorting and a dedicated keyword alert ID. Presentation
timestamps remain unchanged in the disabled-index `display` object.

`build_document(Projection(...))` creates a complete current snapshot, including
current-note search tokens. The authoritative application's decimal-string
counter wire format is converted to long fields. A deletion becomes only
`id`, `tenant_id`, `projection_version` and `deleted=true`; it retains no source,
note or actor content. Every product query explicitly filters `deleted=false`.

`ProjectionWriter.publish` accepts at most 500 documents, validates the 4 MiB
multi-document batch limit and a caller-supplied single-document capability,
and uses `external_gte` with `refresh=wait_for`. It checks equal-version content
and verifies visibility using search, including uncertain version conflicts.
The required `guard` callback must validate the durable immutable projection
work, active index epoch and retired-identity fence before every attempt.
`covered_by_newer` must establish authoritative same-alert coverage; it must not
approve unrelated sequence holes. The returned `VisibilityProof` identifies
covered sequences; it does not mutate SQL progress.

The running worker assembles at most 50 documents and a 4 MiB ordinary bulk per
turn, with an explicitly allocated single-document exception. It streams note
text from PostgreSQL and retains only bounded distinct tokens during assembly.
Already search-visible identical documents are verified without another write.
One process-owned Elasticsearch client serves both hints and reconciliation.
The worker publishes contiguous per-tenant progress and view generation only
after complete coverage. Thirty-second scan claims and conditional ownership
checks prevent duplicate page work and stale-owner progress; isolated five-second
tenant retries preserve failed watermarks while healthy work continues.
Index writer fencing, rebuild holds, catch-up validation and atomic alias cutover
remain application worker/control-plane responsibilities. This adapter does not
claim a completed rebuild implementation.

## Snapshot reads

`SearchReader` requires explicit callbacks for PostgreSQL time, verified
`SearchState`, authorization and a single batch of retained alert identities.
It captures the searchable watermark before opening the PIT, rechecks index
epoch afterward, and reports later state separately. Five-minute encrypted
cursors bind tenant/session visibility, deployment/index/profile epochs, query,
sort, fixed evaluation time, newest PIT ID and last examined sort tuple.
Every page reauthorizes access, rejects partial/inexact results and preserves
the same time and PIT for rows and exact totals.

Purged rows are omitted without an unbounded refill scan, and pagination advances
by the last examined hit. A purge-generation change or an omitted retired row
makes the total unavailable/updating rather than estimating a count. Disposal
closes PITs best-effort; a successful replacement can dispose the previous
cursor. Keep-alive is capped by the remaining absolute cursor lifetime.
The reader returns dependency errors instead of false zeroes.

Assistant-selected implementation profile `appchi-text-unicode15.1-v1` pins
Unicode 15.1.0, the data version supplied by the pinned Python 3.13 runtime.
Text queries tokenize letter/mark/number/underscore runs, apply full case folding
without normalization and AND distinct tokens across message, impact and current
notes. The maximum query-token byte length is derived from all Unicode scalar
case-fold expansions and the 256-code-point query limit. Only source tokens
larger than any possible valid query token are omitted. A mismatched Unicode
runtime gates this capability rather than silently changing query behavior.

## Verification status

Ten unit tests passed against the configured Windows Python 3.13 environment.
They cover full-message projection, strict mappings, tombstone privacy,
case-fold/no-normalization semantics, scope/query/index cursor binding,
absolute expiry, purge holes, partial failures and visibility proof. These
tests use client mocks to exercise control flow and are not service evidence.

The opt-in real-service test is
`backend/tests/compatibility/test_search_elasticsearch.py`. Set
`APPCHI_TEST_ELASTICSEARCH_URL` to an isolated real Elasticsearch endpoint;
it creates and deletes only a uniquely named `appchi-test-*` index. It tests
native-CEL/Elasticsearch result parity, full-length supplementary-Unicode message
search, PIT pagination, full-text folding and tombstone version fences.
It was skipped locally because no test endpoint was supplied. No Elasticsearch
service compatibility, OpenShift deployment, latency or recovery qualification
is claimed from the unit results.

## Private-network qualification commands

Run from the repository root in a network with an explicitly supplied isolated
Elasticsearch test service. The service must support authenticated access via its
supplied URL/environment and the selected official Python client; provision
credentials and trust using the owning team's normal mechanism. Do not point
this test at an index used by the company application.

```powershell
poetry check --lock
poetry sync --all-extras
$env:APPCHI_TEST_ELASTICSEARCH_URL = 'https://SUPPLIED-ISOLATED-TEST-ENDPOINT'
poetry run python -m pytest tests/compatibility/test_search_elasticsearch.py -v
```

```sh
poetry check --lock
poetry sync --all-extras
export APPCHI_TEST_ELASTICSEARCH_URL='https://SUPPLIED-ISOLATED-TEST-ENDPOINT'
poetry run python -m pytest tests/compatibility/test_search_elasticsearch.py -v
```

Replace the endpoint placeholder before execution. A missing endpoint produces
a skipped test, which is not qualification evidence. Record the actual package,
service/image version and test output in delivery evidence. This command checks
the adapter contracts only; multi-writer rebuild fencing, supplied byte/refresh
capacity, failure recovery and the full workload acceptance profile still need
their separately specified integration exercises.
