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

## Cluster and Enterprise (2026-11-02)
Measured against a 3-node Enterprise cluster and a single Enterprise instance with one member's resources; the cluster is
16-45% slower than that single instance, and 0 transient retries in any run. Table, method and caveats: `docs/CLUSTER.md`.
Run the cluster benchmark with `NEO4J_HOST=neo4j://localhost NEO4J_BOLT_PORT=7687 ./gradlew benchmark --args="--server-hardware='...'
--writers=4 --parsers=2 --label=cluster-w4 --out=docs/benchmarks"`.

## The ceiling harness and what it showed (2026-11-02, KANBAN E10-1; Java 25, Micronaut 5)
`./gradlew ceiling` (`CeilingRunner`) replays the server's own statements and rows through N raw driver sessions, with parsing,
grouping, ordering and parameter building done beforehand and not timed. The graph it writes is checked against the server's
(SHA-256 over every tagged node and relationship, spec-tested including that the check can fail). Pinned Community container,
150k messages, 4 CPUs / 2 GB heap, Docker Desktop on a laptop.

**The replay is not a ceiling.** Run interleaved with the real server pipeline (3 repeats, `2026-11-02-ceiling-vs-server.md`), the
server was *faster* than the replay in every case, 104-135% of it, mostly with non-overlapping ranges. Not explained. Ruled out:
a cold store (a warm-up pass is now in the harness), heap pressure (a one-profile run in a fresh JVM was no better), session
reuse (a session per batch made no difference). Untested leading guess: the replay's prepared rows are long-lived objects and
cost more to serialise than the server's freshly built ones (memory locality). Until that is understood the replay measures a
lower bound, so **no "server % of ceiling" figure is claimed**, and the first matrix (`2026-11-02-ceiling-community-4cpu.md`)
is evidence of where the database saturates, not of a maximum.

What it does show, and the server confirms:
* **Hub profiles saturate early whatever the client does.** hubs3 stops near 38k msg/s at 4 sessions (batch 2000), hub-fanout near
  45k, wide near 8k; adding sessions beyond 2-4 does nothing. The statement shape, not the application, is the limit there.
* **Application CPU per message** (parse+group+order+parameter maps, one thread): node-only 6 us, hub-fanout 10, hubs3 20,
  unique-dest 30, wide 42. One parser thread therefore caps unique-dest near 34k msg/s and node-only near 165k, which is
  where those profiles stop scaling with writers (`PARSER_THREADS` > 1 is the lever there, untested).

**Batch size is a real lever, measured through the real server** (`server-java25-w*.md` at batch 500 against `server-batch2000-w*.md`;
single runs, so treat differences under ~5% as noise):

| msg/s | 1 writer, 500 | 1 writer, 2000 | 4 writers, 500 | 4 writers, 2000 |
|---|---|---|---|---|
| node-only | 43,639 | 63,852 (+46%) | 140,919 | 163,455 (+16%) |
| hubs3 | 21,317 | 26,376 (+24%) | 31,151 | 38,769 (+24%) |
| hub-fanout | 32,908 | 44,395 (+35%) | 40,348 | 49,660 (+23%) |
| wide | 7,761 | 7,661 (-1%) | 8,803 | 8,484 (-4%) |
| unique-dest | 12,834 | 13,190 (+3%) | 35,971 | 35,272 (-2%) |

The cost is latency and memory: p99 batch time at 4 writers goes from 40/101/112/392/94 ms to 66/271/329/2,012/262 ms
(node-only/hubs3/hub-fanout/wide/unique-dest), a bad message costs a bisect over a larger batch, and RabbitMQ prefetch grows
with `BATCH_SIZE x (parsers + writers + 1)`. The default stays 500 until E3-14 decides.

**Java 21 -> 25 and Micronaut 4 -> 5 cost nothing measurable** on the hub profiles (1 writer, batch 500, same container, hubs3
20.3k -> 21.3k, hub-fanout 29.4k -> 32.9k, wide 7.6k -> 7.8k, unique-dest 12.7k -> 12.8k). **node-only is the exception to
watch:** 1 writer read 43-46k against 52k on 2026-11-01 (4 writers 141-156k against 118k). That profile is the noisiest
(see above); not resolved, so not called a regression or an improvement.

## Batch size through the real server (E3-14), 2026-11-03
Pinned Community container, 150,000 messages, `--parsers=1`, one run per cell (reports `2026-11-03-e3-14-batch<N>-w<W>.md`). Warm-up was
10 / 3 / 2 / 1 batches for 500 / 2000 / 5000 / 10000, so the 10000 column has about 14 measured batches: its p99 means little.
A first attempt at 300,000 messages failed on the hub profiles with "memory pool limit exceeded" (not a batch-size effect; hubs
with 300k relationships), so 150k it is. Run-to-run spread is large: batch 500 at 1 writer read node-only 32.8k here and 43.6k
on 2026-11-02, so treat differences under about 15-20% as noise on node-only.

| msg/s | 1 writer: 500 | 2000 | 5000 | 10000 | 4 writers: 500 | 2000 | 5000 | 10000 |
|---|---|---|---|---|---|---|---|---|
| node-only | 32,752 | 57,562 | 55,912 | 56,956 | 137,542 | 130,250 | 107,955 | 119,641 |
| hubs3 | 17,031 | 22,951 | 23,663 | 23,483 | 28,695 | 35,985 | 35,085 | 31,638 |
| hub-fanout | 27,462 | 37,806 | 38,027 | 37,254 | 38,930 | 45,385 | 43,474 | 43,813 |
| wide | 7,051 | 7,081 | 7,130 | 6,993 | 8,530 | 8,114 | 8,051 | 8,393 |
| unique-dest | 11,490 | 12,371 | 11,561 | 10,748 | 35,971 | 32,198 | 28,946 | 24,815 |

p99 batch ms at 1 writer, 500 / 2000 / 5000 / 10000: node-only 34 / 60 / 152 / 270, hubs3 80 / 142 / 321 / 640, hub-fanout 38 / 94 /
205 / 392, wide 118 / 534 / 1,001 / 2,288, unique-dest 89 / 277 / 606 / 1,200. At 4 writers wide reaches 1.9 s at 2000 and 3.9 s at 5000.

Reading it: the gain stops at 2000 (nothing above it helps at 1 writer). At 1 writer, 2000 is +35-38% on hubs3 and hub-fanout, +8%
on unique-dest, flat on wide, and large on node-only (inflated by a low 500 cell). At 4 writers it helps only the hub profiles
(+25% hubs3, +17% hub-fanout) and costs unique-dest 10% (32.2k vs 36.0k), and it is flat or negative on node-only and wide; bigger
batches are worse there. Latency grows linearly with the batch. Not measured yet: bisect cost with one bad message in 5,000, the
2-second `bufferTimeout` at low rates, unacked-message memory.

### E3-14 remaining checks, 2026-11-03 (`./gradlew bisect`, `BisectCost`)
**One bad message in a batch** (a nested object where its siblings have a number, so it shares their statement and only Neo4j
rejects it; median of 5 repeats, rewriting nodes that exist; the other messages all commit): 

| profile | batch | bytes/msg | clean ms | with 1 bad message ms | extra |
|---|---|---|---|---|---|
| node-only | 500 / 2000 / 5000 | 295 | 26 / 41 / 93 | 115 / 140 / 212 | 89 / 99 / 119 ms |
| hubs3 | 500 / 2000 / 5000 | 815 | 42 / 103 / 224 | 96 / 180 / 366 | 54 / 77 / 142 ms |
| wide | 500 / 2000 / 5000 | 2,375 | 127 / 451 / 1,093 | 225 / 719 / 1,829 | 98 / 268 / 736 ms |

The bisect grows with the batch but is a fraction of a second to under a second; a batch with one bad message is 1.6-4.4x a clean
batch. Batches full of bad messages were not measured.

**Unacked-message memory** (RabbitMQ prefetch = batch x (parsers + writers + 1), JSON bytes only; the server holds roughly 3-5x
that as objects): at batch 2000 wide messages hold 14 MB (1 writer) to 29 MB (4 writers); at 5000, 36 / 71 MB. Not a constraint.

**The 2-second `bufferTimeout`** was not measured, reasoned: a batch is flushed when it is full or 2 s after its first message,
whichever is first, so a larger batch adds at most that 2 s at low rates and nothing at high rates (it is full in well under 2 s
above 1,000 msg/s at batch 2000). The 2 s is not changed by the batch size. If latency under 2 s at low rates matters, make the
timeout configurable (not done).

**Decision:** the default `RABBITMQ_BATCH_SIZE` is now 2000. Above it nothing gained, latency and memory grow linearly.
