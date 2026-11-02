# Benchmarks

Throughput numbers for `MessageBatchProcessor` (parse + group + write), without RabbitMQ in the loop. Each report in
this folder is self-describing: its Environment section records the Neo4j image digest, CPU/memory limits, heap, page
cache and client hardware, and the harness **refuses to run** if the Neo4j side cannot be described.

## Run it
```
docker compose -f docker-compose.benchmark.yml up -d --wait        # pinned Neo4j on Bolt 7688, 4 CPUs, 8 GB, 2 GB heap, 2 GB page cache
                                                                    # (add --pull never when the image is already local and Docker Hub is unreachable)
NEO4J_BOLT_PORT=7688 ./gradlew benchmark --args="--container=neo4j-bench --messages=150000 --warmup=10 --label=my-change --out=docs/benchmarks"
docker compose -f docker-compose.benchmark.yml down -v              # throw the database away
```
Options: `--profiles=a,b` (node-only, hubs3, hub-fanout, wide, unique-dest), `--writers=N` and `--parsers=N` (0 writers, the
default, parses and writes one batch after the other; 1 or more runs the server's `BatchPipeline`), `--messages`, `--batch`, `--warmup`
(batches excluded from the numbers), `--seed`, `--label`, `--out`, and either `--container=<name>` or
`--server-hardware="<text>"` for a Neo4j that is not a local container (Aura, a VM). The run deletes everything tagged
`SourceSystem=menome_benchmark` before and after each profile; never point it at a database you care about.

The exit status is non-zero if any message fails or the graph ends up with the wrong number of nodes (2 if it refused
to start).

## What a report contains
1. Environment (see above).
2. Results per profile: msg/s, writes/s, p50/p99 batch time, transient retries.
3. Where batch time goes: share of parse / group / index / statement / commit (see `docs/METRICS.md`).
4. Neo4j plans: `PROFILE` of every statement the processor generates for the first batch, once with keys already in
   the graph (MERGE matches) and once with never-seen keys (MERGE creates), with db hits per operator and a flag for
   label scans. Profiling runs in a rolled-back transaction, so it leaves no data.

## Comparing runs: how much noise is there?
Measured on the pinned environment (same machine, same container, 3 runs each, 2026-11-01):

| Messages per profile | Spread of msg/s across 3 runs (max-min as % of mean) |
|---|---|
| 30,000 | 5% (node-only) to **26%** (hub-fanout); wide 23%, unique-dest 22%, hubs3 10% |
| 150,000 | 2.5% to 3.5% (hub-fanout, wide, unique-dest) |

So: **compare runs of at least 150,000 messages**, and treat differences under about 5% as noise. A 30k run is fine for
a smoke test but cannot support a "15% regression" gate. Short runs also read low because the JIT and the database have
not warmed up (node-only: 20.7k msg/s at 30k messages, 34.5k at 150k).

## What this edition can and cannot show
The harness runs against Neo4j Community. It exposes no server metrics over Bolt (`dbms.queryJmx` returns no beans, no
`server.metrics.*` settings), and `PROFILE` reports no page cache statistics. So **server-wide page cache hit ratio
and lock-wait time are not recorded**; contention is observed as `transient retries` (re-runs of a statement after a
lock or deadlock error), which is 0 for a single writer. Backlog story E1-5 adds the server-side numbers for Enterprise.

## Comparing two runs: use the same warm container, and expect order effects
The first profile in a run on a freshly created container is measured against a cold page cache and growing store
files: node-only read 26.8k msg/s on a new container and 29.5-33k on the same container a run later (the baseline in this
folder was taken on a warmed container). To compare a change with a baseline, run both on a container that has already
done a full run, and treat anything inside +-5% as noise.

## Results so far (150,000 messages, pinned environment, warm container)
| Profile | Baseline | After E2-1 + E3-2 (+E9-1 check) |
|---|---|---|
| node-only | 34,469 | 32,605 (-5%, inside noise) |
| hubs3 | 10,979 | 12,974 (+18%) |
| hub-fanout | 20,682 | 22,107 (+7%) |
| wide | 5,244 | 5,770 (+10%) |
| unique-dest | 8,203 | 9,680 (+18%) |

Reports: `2026-11-01-baseline.md`, `2026-11-01-after-e2-1-e3-2.md`.

After E2-3 (`@CompileStatic` on the hot path, environment lookups cached; same container, warm):
node-only 34,899 (+7%), hubs3 15,109 (+16%), hub-fanout 25,170 (+14%), wide 6,107 (+6%), unique-dest 11,076 (+14%)
-> `2026-11-01-after-e2-3.md`.

After E3-1 (uniqueness constraints instead of plain indexes; the run also migrated the existing indexes):
node-only 35,692 (+2%), hubs3 14,800 (-2%), wide 6,149 (+1%), hub-fanout 22,045 in the full run but 24,151 and 24,881 when
repeated (about -3%), unique-dest 9,602 / 10,308 / 9,837 (-8 to -13%) -> `2026-11-01-after-e3-1.md`. The constraint's
uniqueness check is paid on every node create, so the workload that creates the most new nodes per message (unique-dest:
4) pays most. That is the price of the guarantee: 4 concurrent writers on overlapping keys made 412 nodes for 300 ids
with a plain index and exactly 300 with the constraint.

## Scaling with writers (E4-1, E4-2): msg/s, 150,000 messages, pinned Neo4j (4 CPUs), 2026-11-01
`serial` = parse then write on one thread. `1w` = the pipeline: the next batch is parsed while this one is written.
Parsers: 1 for up to 2 writers, 2 for 4 and 8. Transient retries (lock timeouts/deadlocks) were 0 in every run.

| Profile | serial | 1 writer | 2 writers | 4 writers | 8 writers | 8 vs 1 | efficiency at 4 |
|---|---|---|---|---|---|---|---|
| node-only | 39,558 | 52,019 | 86,051 | 118,413 | 131,896 | 2.5x | 57% |
| hubs3 | 14,858 | 20,299 | 26,103 | 30,003 | 29,904 | 1.5x | 37% |
| hub-fanout | 25,817 | 29,441 | 37,230 | 40,363 | 39,547 | 1.3x | 34% |
| wide | 5,886 | 7,567 | 8,537 | 8,572 | 8,323 | 1.1x | 28% |
| unique-dest | 10,181 | 12,709 | 21,167 | 33,856 | 49,577 | 3.9x | 67% |

(Reports: `2026-11-01-serial.md`, `-pipeline-w1.md`, `-pipeline-w2.md`, `-pipeline-w4.md`, `-pipeline-w8.md`.)

* **Overlapping parse and write alone (serial -> 1 writer): +14% to +37%.** The database is the slower stage, so the
  parser keeps it fed. No behaviour change: one writer keeps batches in arrival order.
* **Workloads whose messages do not share nodes scale with writers** (unique-dest 3.9x at 8; node-only 2.5x, limited by
  the 4 CPUs of the Neo4j container and noisy at that rate: an earlier 8-writer run read 181k).
* **Workloads that all point at the same few nodes do not.** A relationship write-locks both ends until commit, so
  batches pointing at the same hub queue behind each other: hubs3 (3 hubs) tops out near 1.5x, hub-fanout (1 hub) near 1.3x,
  and wide, whose 10 activity nodes are hubs for every batch, near 1.1x. More writers do not help; they only wait.
* **Lock order matters more than writer count.** With statements ordered by relationship name but nodes by label, writers
  deadlocked: wide fell from 7.8k to 1.5k msg/s at 2 writers with 70 retries, hubs3 got slower with every writer. One global
  order (label, then key, in both phases) removed every retry and every collapse.
* `RELATIONSHIP_TRANSACTIONS=statement` (one commit per relationship statement, shorter hub locks) was measured and is
  not the default: hubs3 17.3k vs 20.6k with 1 writer, 23.2k vs 25.0k with 2, 30.8k vs 28.7k with 4 (+7%), hub-fanout no gain.
* The Neo4j container has 4 CPUs and shares the Docker VM with the benchmark JVM; a larger server would move the
  saturation point of the scaling profiles, not the hub-bound ones.
