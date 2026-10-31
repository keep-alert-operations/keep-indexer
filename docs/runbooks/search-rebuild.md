# Search rebuild

The application can reconstruct its current search projection without replaying
source lifecycle or correlation. Run the CLI from a privileged application worker
inside the supplied environment. It uses the normal PostgreSQL/Elasticsearch
Settings and their configured TLS trust. It does not set readiness gates to true.

Supply a JSON binding with profile `appchi-es-rebuild-v1`, exact `alias`, an
owner-approved `index_prefix`, `capability_evidence`, `atomic_alias_switch: true`,
`exclusive_alias_writer: true`, `max_document_bytes` (1024–1048576), and explicit
`index_settings`. The settings describe the owning team's supplied allocation;
Appchi does not select its production shard/replica/lifecycle policy. The evidence
must establish exclusive control of this alias and supported atomic remove/add.
These assertions require environment qualification; a valid JSON file alone is
not evidence. `APPCHI_ELASTICSEARCH_INDEX` must be the binding's alias.

Initialize an existing, exclusively owned alias and physical index once:

```powershell
backend/.venv/Scripts/python.exe -m appchi.search.rebuild_cli --binding <binding.json> bind --epoch <fresh-uuid>
backend/.venv/Scripts/python.exe -m appchi.search.rebuild_cli --binding <binding.json> start
backend/.venv/Scripts/python.exe -m appchi.search.rebuild_cli --binding <binding.json> resume <returned-job-uuid>
backend/.venv/Scripts/python.exe -m appchi.search.rebuild_cli --binding <binding.json> status <job-uuid>
```

Use `resume <job> --once` for one bounded page/state transition. Restart the same
job after interruption. Optional `APPCHI_WORKER_HEALTH_PATH` supplies the standard
worker probes. Initial bind resets claimed searchable progress; ordinary indexers
verify it against the captured physical index. The CLI never creates an initial
alias by guessing a service team's allocation or overwrites an unexpected target.

A durable global hold precedes the PostgreSQL snapshot and prevents physical
projection-log cleanup, including records for newly created tenants. A captured
position and keyset cursor make copying resumable. Subsequent complete snapshots
and retirement markers catch up by tenant sequence. During cutover, writers pause
and drain their bounded leases; canonical ingestion continues. Every indexed
ID/version/content is checked against the authoritative cutover snapshot, together
with strict mapping/profile and exact cardinality. This provides document parity;
service-specific representative CEL/count and capacity qualification remain in the
private verification plan.

The alias switches with one supported atomic remove/add operation. A lost response
keeps the job in `switching`, search paused, and the hold active. Resume reads the
actual alias before committing the new epoch/progress. An unrelated search gate
remains blocked throughout. Old PITs restart with `index_changed`. Indexers always
write their captured physical index, so delayed work cannot target the new alias.
The prior index remains for owner-controlled repair/cleanup.

Before `switching`, `cancel <job>` verifies the alias still serves the old index,
fences the attempt, releases the rebuild hold and resumes its old epoch. It keeps
the unused target index for owner cleanup. Cancellation is rejected once switching
may have happened; reconcile the same job instead. Never edit job state, lower
versions, delete its held projection records, or reopen search by toggling a gate
to conceal an unresolved switch. A full PostgreSQL rollback restore still requires
the separate independent evidence/session/credential reconciliation workflow;
a successful search rebuild alone does not establish safe restore readiness.

The isolated local development stack uses the same CLI with its fixed local binding;
its search bootstrap then accepts the completed target index. See delivery's local
runtime guide, "Search mapping changes".

Adapter references: [Elastic aliases](https://www.elastic.co/guide/en/elasticsearch/reference/current/aliases.html)
and [atomic alias action API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-update-aliases).
