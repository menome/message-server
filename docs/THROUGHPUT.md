# Throughput assessment

Goal: find the maximum import throughput this message server could reach against Neo4j, what limits it today, and
what it would take to get there. **Measured** numbers come from this repository's own tests. **Estimated** numbers are
a model and must be validated by the benchmark harness story (KANBAN E1) before anyone quotes them.

## 1. Pipeline and where time goes

```
RabbitMQ queue -> bufferTimeout(500, 2s) -> parse+validate -> group by signature -> UNWIND/MERGE per group
(consumeManualAck)   (1 consumer)           (CPU, 1 thread)    (CPU, 1 thread)      (1 session, serial txs)
```

Today everything after the buffer runs **serially on one thread with one Neo4j session**; a batch must be committed
before the next one is parsed (`concatMap(.., 1)`). A 500-message batch of the sample employee message (1 primary
node, 3 connection nodes, 3 relationships) issues **7 write transactions**: 1 primary-node group, 3 connection-node
groups (different labels/keys), 3 relationship groups.

## 2. Measured baseline

> Superseded by the benchmark harness: see `docs/benchmarks/2026-11-01-baseline.md` (pinned environment, five profiles,
> stage breakdown, plans). The table below is the earlier one-off micro-benchmark, kept for the cost model.

Environment: developer laptop (16 cores, Docker), Neo4j 2026.10.1 Community in Testcontainers, RabbitMQ in the same
Docker VM, one message-server instance, batch size 500, sample employee message. Source: the 5,000 and 50,000
message end-to-end specs in `MessageServerCommandSpecification` (batch logs) and a one-off micro-benchmark.

| Quantity | Value |
|---|---|
| Steady-state batch (500 msgs) | ~60 ms, i.e. **~8,000 msg/s** (range 6,000-8,800) |
| 50,000 messages end to end | 9.9 s including ~3 s server start-up and drain |
| Graph writes per message | 1 node + 3 relationships (+ 3 shared hub-node merges per batch) |
| Graph writes/s at steady state | ~32,000 (8k nodes + 24k relationships) |
| Parse: schema validation | ~7.6 us/msg |
| Parse: JSON -> node/connection objects | ~7.1 us/msg |
| **Grouping keys (`signature()`/`identity()`)** | **~34 us/msg** - the largest app-side cost, ~17 ms of every 60 ms batch |
| App CPU total | ~49 us/msg -> ~20k msg/s single-thread ceiling, serial with the DB time |
| Database + network time per batch | ~35 ms for ~2,000 writes -> ~57k writes/s from one writer thread |

Caveats: the client and server share one machine; JIT warm-up inflates the first batches; the sample workload is
friendly (small, uniform messages, 3 hub nodes). The micro-benchmark was not kept in the repo - E1 turns it into a
permanent benchmark.

## 3. Model

Per-message cost: `c_app` (CPU) + `w_n` node writes + `w_r` relationship writes. For a batch of size `B` with `W`
concurrent writers:

```
T_total = min( T_app , T_db , T_broker )
T_app   = cores_for_app / c_app
T_db    = W_eff * rate_per_writer / (w_n + w_r)        W_eff = W * efficiency(contention)
T_broker= sharded-queue consumer rate
```

* **Today:** `W = 1` and app and DB are serial, so `1 / (c_app + c_db)` ~ 1 / (49 us + ~70 us) ~ 8.4k msg/s. This
  matches the measurement, so the model is at least consistent.
* **Overlap only (no new writers):** parse batch N+1 while batch N commits -> `1 / max(c_app, c_db)` ~ 14k msg/s.
* **Cheaper keys** (cache signature/identity, ~34 -> ~2 us): c_app ~ 17 us; no change while DB-bound, but required
  before adding writers, otherwise the single parse thread caps at ~58k msg/s.
* **Parallel writers** (4-8): writer efficiency for MERGE-heavy work with uniqueness constraints is typically
  60-80%, lower with hub contention. **Estimated 35-45k msg/s at 4 writers, 60-90k at 8** (240-360k graph writes/s)
  on a 16-core NVMe box, if relationship writes are partitioned so concurrent writers do not lock the same hubs.
* **Hub contention is the swing factor.** A relationship write locks both endpoints. In the sample workload every
  message points at the same 3 Office/Project/Team nodes, so naive concurrency serializes on them (and deadlocks).
  Partitioning (below) turns this into the 60-80% case; without it, expect little gain beyond 2 writers.

### Where the hard ceiling is (estimated)
1. **Single write leader.** Clustering adds read capacity and availability, not write throughput.
2. **Transaction log and page cache.** 80k msg/s x ~1 KB of log per message is ~80 MB/s sequential - fine for NVMe,
   binding on network disks. The working set of the indexes/constraints must fit in the page cache or MERGE lookups
   turn into disk reads.
3. **RabbitMQ.** One classic queue sustains roughly 20-50k msg/s per consumer path; beyond that shard across queues.
4. **Beyond ~100k msg/s per database:** use the offline/incremental bulk path (`neo4j-admin database import`,
   millions of records/s) for backfills, and keep the message server for the trickle.

## 4. Neo4j features that matter (Cypher manual, "CALL subqueries in transactions")

Checked against the current manual:

* `CALL (row) { ... } IN TRANSACTIONS OF n ROWS` commits every `n` input rows (default 1000) and bounds
  transaction-state memory. `ON ERROR CONTINUE | BREAK | FAIL | RETRY [FOR n SECONDS] [THEN CONTINUE|BREAK|FAIL]`
  and `REPORT STATUS AS s` expose per-batch `committed` / `errorMessage`. This can replace most of the app-side
  bisecting: let the server skip the failed inner batch, then re-run only that batch at smaller size.
* `IN [n] CONCURRENT TRANSACTIONS` runs inner batches in parallel (default from CPU cores; slotted runtime;
  order undefined, so only for independent batches). Concurrent batches that lock the same nodes deadlock; the
  server rolls back and retries, which is slow at scale.
* **`DISJOINT BY { NONE | AUTO | (<expr>, ...) }`** (Cypher 25, introduced in Neo4j **2026.06**; default via
  `dbms.cypher.transactions.default_subquery_batch_strategy`) schedules batches that share a lock-prone value
  sequentially and everything else concurrently. `AUTO` infers the expressions from `MERGE` patterns and needs
  indexes on the merged properties (`EXPLAIN` shows what it inferred); explicit expressions may only use imported
  variables; `NONE` is right when uniqueness constraints already make rows independent.
* Documented pattern for imports with shared nodes: concurrent subquery for node MERGEs, then a serial (or
  disjoint-partitioned) subquery for relationships; use key constraints so MERGE lookups are index-backed and
  correct under concurrency. Relationship phase parallelism defaults to 1 in Neo4j's own connector docs.
* "Mix and batch" (Neo4j developer blog): partition relationship rows into groups whose start-node and end-node sets
  are disjoint, run the groups in parallel, mix them across rounds.

## 5. Path to the maximum, in order of payoff

1. Measure properly (E1) so every later step has a before/after.
2. Stop wasting the writer: cache keys, overlap parse with commit (E2, E4) -> ~14k msg/s.
3. Fewer, bigger transactions and server-side error isolation (E3) -> removes ~6 of 7 commits per batch.
4. Constraints + node-phase concurrency (`DISJOINT BY AUTO`/`NONE`) (E3) -> first multi-writer gain.
5. Hub-aware relationship partitioning (E3, E4) -> keeps concurrency efficient on real data.
6. Sharded queues / consumers, Neo4j memory and log tuning (E5, E6) -> push toward the 60-90k estimate.
7. Bulk path for backfills (E7) for anything above that.

Status 2026-11-01: steps 1-4 are implemented and measured (E1, E2, E3-1, E3-2, E4-1, E4-2); the measured results are in
`docs/benchmarks/README.md`. The model above was right about the shape (overlap, fewer commits, writers scale until they meet
shared nodes) and too pessimistic about the single writer: grouping got so much cheaper that one writer with overlap reaches
about 20k msg/s on the sample message, against the modelled 14k. Hub contention (step 5) is not solved: see E3-7.
