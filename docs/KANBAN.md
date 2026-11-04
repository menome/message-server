# Kanban: message-server throughput

Single source of truth for planned work (kanban lives in MD files in git, by standing decision). Move a story by
cutting it to the next column. Priority: P0 do first, P1 next, P2 later. Size: S <1d, M 1-3d, L 3-5d.
Background and numbers: `docs/THROUGHPUT.md`, measured results in `docs/benchmarks/README.md`. Last updated 2026-11-05
(P0: E3-7 partly done, plus E10-1 in progress). Move a story to In progress when you start it and to Done, with what was measured, when you finish.

## Goal
Take the original Menome data atomization pattern (harvesters emit atomised JSON messages, a refinery merges them into a
knowledge graph, continuously) to current code and methods, then make ingestion as fast as it can be, ideally faster than
any alternative. Where each part stands is in `RESUME.md` ("Scorecard"). Short version: modernisation is largely done,
correctness and cluster behaviour are verified, speed is 1.4-1.9x the old code by default (up to 4.1x with 4 writers) on a
laptop; **"faster than anything else" is not yet shown** because the ceiling and the alternatives have not been measured
(E10-1, E10-2) and nothing has run on server-class hardware (E10-3).

## Done

| Story | Notes |
|---|---|
| Fix bisect split losing nodes on odd batch sizes | `MessageBatchCollection.split`; unit-tested for sizes 2-501 |
| One Cypher statement per signature (label + dimension keys + property keys) | mixed-property batches no longer drop properties |
| Direction, RelProps/DateRelProps in connection signature | no more reversed edges |
| Report relationship errors; fix success count | `successCount = messages - failed messages` |
| Integer dimensions stay Long | Gson `LONG_OR_DOUBLE` |
| Last message for a node in a batch wins | |
| Process schema features: DateProperties, Label, Delete*, RelProps | previously accepted then ignored |
| Backtick-quote all identifiers (Cypher injection) | schema also rejects empty NodeType/RelType |
| Bisect only on data errors; infrastructure errors requeue the batch | `isDataError` |
| Manual acks after commit, error queue (`<queue>.errors`), stream retry with backoff | at-least-once, idempotent MERGE |
| Close drivers, sessions, channels; one driver per process | |
| Test endpoints off by default, POST, bounded | `ENABLE_TEST_ENDPOINTS` |
| Remove Card-pattern remnants and Redis | |
| Fix CASE_STYLE | |
| Pin every statement to `CYPHER 25` (`Cypher.versioned`); compose sets `db.query.default_language=CYPHER_25`; test proves it works on a Cypher-5-default database | 2026.10.1 is the newest 2026.x image and driver 6.3.0 the newest driver as of 2026-10-28 |
| Index syntax for Neo4j 5+/2026 (`CREATE INDEX IF NOT EXISTS`), `randomUUID()` instead of APOC | |
| Upgrade: Java 21, Gradle 8.14, Micronaut 4.10, Neo4j driver 6.3.0, Testcontainers 2.0, Neo4j 2026.10.1 | prod jar 31 MB |
| CLAUDE.md, RESUME.md, THROUGHPUT.md, this board | |
| E1-1 Benchmark harness | `./gradlew benchmark --args=...` (source set `src/benchmark`), 5 profiles, fails on errors or wrong node count; first run in `docs/benchmarks/2026-11-01-baseline.md` (single 30k-message run per profile, no repeat-run variance yet) |
| E1-2 Per-stage metrics | `messageserver.stage.duration{stage=parse/group/index/statement/commit/ack}` (`StageTimings`, `docs/METRICS.md`); stages explain 97-98% of batch time in every profile; spec-tested. Also fixed: `ENABLE_METRICS=Y` never worked (application.yml disabled Micronaut metrics, so no MeterRegistry bean) |
| E1-3 Reproducible environment | `docker-compose.benchmark.yml` (image digest, 4 CPUs/8 GB, 2 GB heap + page cache); the harness refuses to run without `--container` or `--server-hardware` and echoes image digest, limits, Neo4j settings and client hardware into the report; measured noise: 22-26% spread at 30k messages, 2.5-3.5% at 150k, so compare runs of >=150k (`docs/benchmarks/README.md`) |
| E1-4 Neo4j-side evidence per run | every statement of the first batch of each profile is `PROFILE`d (match and create path) in a rolled-back tx: plan, db hits, scan flag, in the report; `messageserver.write.retries` counts transient retries as the contention signal. Community exposes no server metrics over Bolt, so server-wide page cache ratio and lock waits moved to E1-5 |
| E5-6 Cluster correctness | `isDataError` is an allow-list (`Statement.*`, `Schema.*`, driver value conversion); `Cluster.*`, read-only/unavailable DB, security, timeouts and unknown codes requeue the batch. Unit test per error code, and a fake-session test that a `NotALeader` propagates with nothing committed or dead-lettered. **Not yet done:** a failover test against a live cluster (tracked in E5-3) |
| E9-1 Reserved property names | `Neo4JNode.assertNoReservedProperties`: `Uuid`/`TheLinkAddedDate` and Name/Label/Priority/SourceSystem (when the message has that field) cannot be set via Properties, DateProperties, DeleteProperties; `Uuid`/`TheLinkAddedDate` not as dimensions either. Rejected as a bad message. `SourceSystem` in a connected node's Properties stays legal. Follows CASE_STYLE |
| E2-1 Compute signature/identity once | cached per object (`signature()`, `identity()`, `dimensionKeys()`), signature built from key names without the values map. Grouping cost per message (150k run): node-only 2.6 -> 2.7 us, hub-fanout 4.6 -> 3.3, hubs3 9.3 -> 6.9, unique-dest 10.1 -> 7.1, wide 13.5 -> 9.5. **Acceptance (< 5 us) met for 2 of 5 profiles**; the rest is Groovy dynamic dispatch, moved to E2-3. A StringBuilder rewrite was tried and was slower, so reverted |
| E3-2 One transaction per phase | all node merges in one write tx, all relationship statements in another, deletes in a third (`executePhase`): the sample message needs 2 instead of 7. A data error rolls the phase back and re-runs it statement by statement, then bisects (same results as before, spec-tested with a rollback-modelling fake and the existing real-database specs). Commit time per message: hubs3 30.7 -> 17.9 us, unique-dest 44.6 -> 30.4. Throughput vs baseline (E2-1 + E3-2 + E9-1 together): hubs3 +18%, unique-dest +18%, wide +10%, hub-fanout +7%, node-only -5% (noise) |
| E2-3 Static compilation | `@CompileStatic` on the parser, node, connection, collections, processor, `StageTimings` and `ApplicationConfiguration` (which also reads each environment variable once: `System.getenv(name)` allocated on every call, three times per node); per-message loops use plain loops, no closures. vs the previous report on the same warm container: node-only +7%, hubs3 +16%, hub-fanout +14%, wide +6%, unique-dest +14%; parse+group per hubs3 message 24.7 -> 16.7 us. Grouping < 5 us/msg now holds for node-only (1.6), hub-fanout (2.3), hubs3 (4.9); **not** for unique-dest (5.7) and wide (8.0, 41-property nodes): see E2-6 |
| E3-1 Uniqueness constraints | `SchemaManager`: a uniqueness constraint on each node type's dimension keys replaces the plain index (`KEY_CONSTRAINT=unique|nodekey|index`). An old plain index is dropped to make room (migrated 8 labels on the benchmark database); existing duplicates fall back to a plain index + ERROR log with the finder query, remembered so DDL is not retried per batch; a node key falls back to unique where unsupported. Spec-tested with a fake session and against a real Neo4j: concurrent writers on overlapping keys made 412 nodes for 300 ids with a plain index, exactly 300 with the constraint (3 of 3 control runs failed without it). Constraint violations are data errors (`Schema.*`). Cost: unique-dest -8..-13%, hub-fanout ~-3%, others within noise. **Found on the way:** Neo4j reports a failed constraint creation as a DatabaseException, which the first version (and a fake that threw ClientException) missed. **Not verified:** NODE KEY on Enterprise (E3-12) |
| E3-12 Verify E3-1 on Enterprise | against a real 3-primary cluster (`ClusterSpecification`, `docs/CLUSTER.md`): constraint DDL through a routing driver reaches the leader and is visible on every member; `KEY_CONSTRAINT=nodekey` creates a `NODE_KEY`. **Found and fixed:** Neo4j refuses a node key next to a uniqueness constraint on the same keys, and the reverse (`22N66 conflicting constraint already exists`), so `nodekey` silently did nothing over an existing constraint and switching back to `unique` would have failed into the plain-index fallback. `SchemaManager` now checks what exists first, swaps uniqueness for a node key on Enterprise (restoring it if that fails), and leaves Community untouched (16 fake-session specs, a Community spec, an Enterprise spec) |
| E5-3 Cluster routing and failover | `neo4j://` routing verified; a write to a follower is `Neo.ClientError.Cluster.NotALeader` (a `ClientException`) and is requeued, never dead-lettered (E5-6 confirmed live). Leader stopped (12 s) or killed (37 s) under 20,000 messages / 2 writers: all written exactly once, 0 bad data, absorbed by the driver's 30 s retry. **Quorum lost for 55 s** (leader + follower killed) with the real server and RabbitMQ, 200,000 messages (`scripts/cluster-quorum-loss-e2e.sh`): 1-2 batches failed and were requeued by the server, the stream resumed by itself, 200,000 distinct employees, 600,000 relationships, queue empty, nothing in the error queue: `RESULT: PASS`. **Cost of a cluster:** 16-45% less write throughput than a single instance with the same resources (`docs/CLUSTER.md`) |
| E4-1 Overlap parse and write | `MessageBatchProcessor.process` split into `parse` (CPU) and `write` (database); `BatchPipeline` parses the next batch(es) while one is written, used by the server (`PARSER_THREADS`) and the benchmark (`--writers=1`). Batch duration is now the batch's own work (parse + write), throughput is wall clock. serial -> pipeline with 1 writer: node-only +31%, hubs3 +37%, wide +29%, unique-dest +25%, hub-fanout +14%. Same ack order. Spec-tested with gates (next batch parsed while the previous write is stuck, bounded read-ahead, order kept, failure handling) and end to end against the real server. RabbitMQ prefetch sized to the batches in flight |
| E7-3 + E8-1 Latest of everything | 2026-11-02, `docs/DEPENDENCIES.md`: Java 21 -> 25 (newest LTS; Dockerfile, GitHub workflow, Jenkinsfile), Gradle 8.14.3 -> 9.8.2, Shadow 9.6.1, Micronaut 4.10.22 -> 5.2.4 (plugin 5.0.2, which itself requires a JVM 25), Groovy 4 -> 5.0.8, Spock 2.4-groovy-5.0; every library already the newest release, Neo4j 2026.10.1 and the driver 6.3.0 still current. No source change needed; suite 199 tests, 0 failures, 10 skipped. **Deferred:** Groovy 6.0.0 (the Micronaut 5.2.4 BOM is on 5.0.8) |
| E3-14 Batch size default | 2026-11-03: `RABBITMQ_BATCH_SIZE` default 500 -> 2000. Measured through the real server at 500/2000/5000/10000 x 1/4 writers: nothing above 2000 helps; 2000 gives +17-38% on hubs3/hub-fanout, flat on wide, -10% (one run; -2% in another) on unique-dest at 4 writers. One bad message costs 54-736 ms extra; unacked memory <= ~71 MB; the 2 s `bufferTimeout` is unchanged. Tables in `docs/benchmarks/README.md` |
| E9-10 Vendor-neutral names (part 1) | 2026-11-04: the id and creation-timestamp properties the server writes are config (`ID_PROPERTY`, `CREATED_AT_PROPERTY`; defaults `Uuid` / `CreatedAt`, `none` switches one off, any other value is used as written, so `CREATED_AT_PROPERTY=TheLinkAddedDate` keeps existing graphs working); reserved-property rejection follows the configured names; markers `test_framework` / `benchmark`, neutral sample data, default RabbitMQ credentials `messageserver`; README upgrade note. 5 specs in `ManagedPropertyNamesSpecification`. Also fixed `ApplicationConfigurationSpecification`, which wiped every JVM system property and so hung the Gradle test worker when it failed. Suite 211 tests, 0 failures, 10 skipped |
| E8-8 Commit history | 2026-11-05: the three oldest local commits, which did not compile alone, are squashed into one (`69be69c`); it and `4e20a01` were built in a clean worktree (`compileGroovy compileTestGroovy`). The cluster work is committed |
| E4-2 Parallel writers | `WRITER_THREADS` (default 1). Scaling curve 1/2/4/8 writers in `docs/benchmarks/README.md`: unique-dest 3.9x at 8, node-only 2.5x, hubs3 1.5x, hub-fanout 1.3x, wide 1.1x; 0 transient retries. Needed one global lock order (label, then key, both phases: `LockOrder`, ordered statements) after the first version deadlocked (wide 7.8k -> 1.5k msg/s at 2 writers, 70 retries). `SchemaManager` DDL serialised. Real-database specs: 4 writers on heavily shared nodes write everything once with no failures, in both transaction scopes, and a batch delivered twice concurrently changes nothing. **Caveat, by design:** with more than 1 writer two batches carrying the same node can commit in either order (E4-3 fixes this with key-aware routing); default stays 1 |

## In progress

| Story | Notes |
|---|---|
| E10-1 The ceiling | started 2026-11-02, **not finished**. Built: `CeilingRunner` (`./gradlew ceiling`, source set `src/benchmark`) replays the server's own statements and rows through N raw driver sessions; graph equality with the server verified by fingerprint (spec-tested, including that the check can fail); interleaved server-vs-replay comparison with repeats; matrix of batch 500/2000/10000 x 1/2/4/8 sessions (`docs/benchmarks/2026-11-02-ceiling-*.md`, README section). **Result: the replay is not a ceiling**: the real server beat it in all 10 interleaved cases (104-135%, `ceiling-vs-server`), cause unexplained (cold store, heap pressure and session reuse ruled out; memory locality of prepared rows is the untested guess), so no "server % of ceiling" is claimed. **What it did establish:** hub profiles saturate near 38k (hubs3), 45k (hub-fanout) and 8k (wide) whatever the client does; app CPU per message is 6-42 us so one parser caps unique-dest near 34k; and **batch 2000 beats 500 in the real server by +16..46% on node-only, hubs3 and hub-fanout, nothing on wide and unique-dest** (E3-14). **Left to do:** explain the replay's deficit (feed rows from a producer thread instead of pre-built memory) or define the ceiling differently, then fill in "gap split" |

## Backlog

### E1 Measure first
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E1-5 | Server-side evidence on Enterprise/Aura: scrape page cache hit ratio, lock wait time, checkpoint and tx-log metrics (Prometheus/JMX) before/after each profile and add them to the report; skipped with a note on Community | P2 | S | report shows page cache hit % and lock waits when the server exposes them |
| E1-6 | Benchmark and test cleanup that survives large hubs: `DETACH DELETE` of a hub with 400k+ relationships exhausts a 1 GB-heap member (`51N72`); delete leaf nodes first, in batches, then the hubs (`Neo4J.deleteAllTestNodes`, `BenchmarkRunner.cleanup`) | P2 | S | cleanup of 500k messages works on the cluster members |
| E1-7 | Controlled Community vs Enterprise comparison: a single Enterprise instance with 2 CPUs / 1 GB heap matched Community with 4 CPUs / 2 GB on hub-bound profiles (hubs3 32.9k vs 30.0k at 4 writers) and was lower on node-creating ones. Unexplained; same CPUs, heap, page cache and store format on both, one variable at a time | P2 | S | report names the cause (or says it is noise) |

### E10 The goal: fastest possible, and proof of it
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E10-2 | **Comparables: the other live-ingestion patterns.** Load the same profiles through (a) the Neo4j Connector for Kafka (sink, Kafka + Connect) and (b) the Neo4j Connector for Apache Spark (batch and structured-streaming writes), each configured by its own documentation and then tuned fairly (batch size, parallelism, the same constraints). `neo4j-admin database import` is deliberately **not** a competitor: it is an offline file-level load that bypasses transactions and logs, so it is not comparable; it stays a separate reference for initial loads (E7-1). **Comparability rules:** same pinned database container and same Neo4j settings; same message content and the same resulting graph, verified by counts and a content hash; durability kept on (transaction logs, commit before ack/offset commit); the whole pipeline of each tool (broker + worker) gets the same CPU and memory budget as RabbitMQ + this server; each run reports msg/s, msg/s per core and peak memory, plus the setup effort and failure behaviour (what happens on a failover and on a bad message). Absorbs E7-2 | P1 | L | one table per profile: ours vs Kafka connector vs Spark connector on equal resources; the claim "faster than X" is made only for tools in it, with the configs published |
| E10-3 | **Server-class run, on a comparable base.** Everything so far is Docker Desktop on a laptop with client, broker and database sharing it. Run the benchmark, the cluster and the E10-2 comparables on the same dedicated machines (or one cloud instance type with local NVMe, database on its own host), 1M+ messages, 1 / 4 / 8 writers. Scripted environment (compose or Terraform) so any tool's run can be repeated on identical hardware; hardware, network and disk stated in every report | P1 | M | report with hardware stated; numbers labelled laptop vs server; every E10-2 tool measured on the same base |
| E10-4 | **Through the broker at scale.** The benchmark drives the pipeline from memory; production goes through RabbitMQ. Measure publisher rate, queue depth, consumer prefetch, ack latency and sustained rate with 1M+ messages in the real server; a soak of several hours for memory and throughput drift | P1 | M | sustained msg/s through RabbitMQ within 10% of the in-memory pipeline, flat memory over the soak |

### E2 App-side CPU (measured: ~34 us/msg in keys, ~15 us parse+validate)
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E2-2 | Parse each message once: validate the Gson tree (or JSON Schema validator on Jackson tree) instead of org.json then Gson | P1 | M | no `org.json.JSONObject` in the hot path; same validation errors; ~7 us/msg saved |
| E2-4 | Cache generated Cypher per signature (map signature -> statement) | P1 | S | `toCypher()` called once per signature per process |
| E2-5 | Avoid per-row `HashMap` + boxing when building `$parms` (reuse maps / typed parameter builders) | P2 | S | allocation rate per msg down in E1 |
| E2-6 | Shape cache: when a message has the same key shape as the previous one, reuse its signature instead of rebuilding the string (nodes with many properties pay most) | P2 | S | grouping < 5 us/msg for wide and unique-dest in the E1 harness |

### E3 Cypher and transaction shape (Neo4j 2026.06+, Cypher 25)
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E3-3 | `CALL (row) { ... } IN TRANSACTIONS OF n ROWS` for large UNWIND batches; tune `n` (target 10-50k rows per tx); add memory guard via `db.memory.transaction.max` | P1 | M | batch sizes of 50k messages stay within heap; benchmark picks `n` |
| E3-4 | Server-side error isolation: `ON ERROR CONTINUE REPORT STATUS AS s`, then re-run only failed inner batches (bisect from that size down) instead of bisecting from the whole batch | P1 | M | 1 bad message in 5,000 costs ~ log2(n/inner) extra statements, not ~2x batch; bad rows still reported with original message |
| E3-5 | `ON ERROR RETRY FOR n SECONDS THEN CONTINUE` for transient lock/deadlock errors inside concurrent subqueries; align with driver retry (avoid double retry) | P1 | S | deadlock injection test: batch completes, no message lost |
| E3-6 | Node phase concurrency: `CALL (row) { MERGE ... } IN n CONCURRENT TRANSACTIONS OF b ROWS` with `DISJOINT BY AUTO` (needs E3-1 indexes) or `DISJOINT BY NONE` (with constraints); verify with `EXPLAIN` that inferred expressions match dimension keys | P1 | L | node-only profile scales with n; zero deadlock errors in 1M-message soak; requires slotted runtime noted in docs |
| E3-7 | _Partly done in E4-2: one global lock order removed all deadlocks, and `RELATIONSHIP_TRANSACTIONS=statement` (shorter hub locks) was measured: +7% at 4 writers on hubs3, -16% at 1, so it is opt-in. **The acceptance below is not met for hub profiles**: efficiency at 4 writers is 37% (hubs3), 34% (hub-fanout), 28% (wide) against 67% for unique-dest; a relationship write-locks the hub until commit, which no scheduling removes. Remaining ideas: E3-8 (super-node strategy), E3-9 (CREATE instead of MERGE for known-new sources), E3-13 (DISJOINT BY, mainly for deadlock avoidance)._ Relationship phase: serial subquery by default; hub-aware parallel option: `DISJOINT BY (param.`d:Key`)` for hub destinations, or mix-and-batch partitioning of rows into groups with disjoint endpoint sets | P0 | L | hub profile (E1-1c) shows >= 60% scaling efficiency at 4 writers vs 1; no deadlocks |
| E3-8 | Super-node strategy: detect high-degree destinations, MERGE relationship from the lower-degree side, consider relationship-type-specific intermediate nodes or time-bucketing for unbounded fan-in | P2 | M | documented thresholds; benchmark with 1M edges into one node |
| E3-9 | Initial-load mode: `CREATE` instead of `MERGE` for known-new keys after in-app dedupe; skip `ON MATCH` work | P2 | M | flag `LOAD_MODE=initial`; guarded by constraint so wrong assumption fails loudly |
| E3-10 | Use `EXPLAIN` in tests to assert every generated statement uses `NodeIndexSeek`/`NodeUniqueIndexSeek` (no label scans) | P1 | S | test fails if a signature produces a scan |
| E3-11 | Batch the index/constraint DDL at start-up from observed signatures and handle `IF NOT EXISTS` races; wait with `db.awaitIndexes` before first batch | P1 | S | no merges against `POPULATING` indexes |
| E3-13 | **Evaluate `DISJOINT BY` for the write phases.** `CALL (row) { MERGE ... } IN n CONCURRENT TRANSACTIONS OF b ROWS DISJOINT BY (expr, ...) ON ERROR RETRY FOR t SECONDS THEN FAIL` (Neo4j 2026.06+, Cypher 25) runs batches that share a declared resource one after another and independent batches concurrently, which prevents the deadlocks of concurrent MERGE-heavy imports. Forms: explicit `DISJOINT BY (row.a, row.b)` (the complete resource list, nothing is added for you; each expression is a separate conflict key, so batches sharing `a` OR `b` are serialized, not only the same pair), `DISJOINT BY AUTO` (inferred from the MERGE patterns, needs indexes on the merged properties, check with `EXPLAIN`; no restriction if nothing can be inferred), `DISJOINT BY NONE` (off, overrides the database default). Only valid with `IN CONCURRENT TRANSACTIONS`, needs an implicit (auto-commit) transaction. **Checked 2026-11-01: explicit and AUTO both run on Community 2026.10.1.** Approach: put the node phase and the relationship phase in such a subquery, start with AUTO + EXPLAIN, then explicit `(row.\`d:Key\`, ...)` on the shared (hub) end, and compare with the application-level writers of E4-2 (which already avoid deadlocks by one global lock order). Expect it to help deadlock avoidance more than hub scaling: batches that all point at one hub are serialized by definition. Trade-offs to weigh: inner batches commit independently (not atomic with the outer statement, so the error isolation of E3-4 changes), and rows per inner batch replace our bisecting. Reference: Cypher manual "CALL subqueries in transactions"; Neo4j data ingestion guide, "Transactional and concurrent Cypher ingestion". Related: E3-4, E3-6, E3-7 | P2 | M | benchmark of hubs3, wide and unique-dest against the E4-2 writer curve with 0 deadlock retries; decision recorded (adopt, adopt for one phase, or keep application-level writers) |

### E4 Pipeline concurrency
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E4-3 | Key-aware routing so messages for the same node land on the same worker (hash of label + dimensions) to avoid cross-worker lock conflicts and keep per-key ordering | P1 | M | no two workers write the same node key concurrently (asserted in test) |
| E4-4 | Adaptive batch sizing: grow to the target transaction time (~100-300 ms) / row budget, shrink on error or timeout | P2 | M | stable latency under varying message size |
| E4-5 | Backpressure and bounded memory: prefetch tied to workers x batch, graceful shutdown that drains/acks in-flight batches | P1 | M | kill -TERM during load loses nothing; memory flat in soak |
| E4-6 | Async/reactive driver sessions (`executeWriteAsync`/reactive) to pipeline round trips instead of blocking threads | P2 | L | CPU idle time during commit measured and reduced |

### E5 Neo4j server tuning
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E5-1 | Sizing guide: page cache >= index+hot store size, heap for transaction state (rows per tx x concurrency), `server.memory.*` | P1 | S | documented formulas validated against E1 runs |
| E5-2 | Transaction log and checkpoint tuning (rotation size, `db.checkpoint.iops.limit`, retention) and disk requirements for target msg/s | P1 | M | no checkpoint stalls visible in soak |
| E5-4 | Aura / managed-service profile: limits on concurrent transactions and tx size, adjusted defaults | P2 | S | doc + config preset |
| E5-8 | Driver max transaction retry time as a setting (`NEO4J_MAX_RETRY_SECONDS`, default 30). A failover took 12 s (graceful) to 37 s (killed); the driver rides through up to 30 s, longer outages reach the requeue path (verified safe). Decide whether a longer window is better than requeue churn, and make it configurable | P2 | S | test: a 45 s outage is either absorbed or requeued, as configured |


### E6 RabbitMQ
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E6-1 | Prefetch/qos tuning and consumer count experiments; document the knee | P1 | S | in E1 report |
| E6-2 | Queue sharding (consistent-hash exchange or N queues) so several message servers scale reads; per-key affinity matches E4-3 | P1 | M | 2 instances x 4 workers measured |
| E6-3 | Quorum queue vs classic/stream trade-off for durability vs throughput | P2 | M | decision recorded |
| E6-4 | DLQ replay tool (`<queue>.errors` -> main queue after fix) and alert on error-queue depth | P1 | S | CLI/endpoint + doc |
| E6-5 | Configurable exchange type and binding (removes hard-coded `direct` + routing key `""` in `RabbitMQ.connectionOk`) | P2 | S | env vars; tests |

### E7 Alternative / complementary paths
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E7-1 | Bulk backfill path: `neo4j-admin database import full/incremental` generator from the same message format | P2 | L | millions of records/s documented for offline loads |
| E7-2 | _Folded into E10-2 (measured comparison on equal resources)._ Evaluate Neo4j Connector for Kafka / Spark for sustained very high volume | P2 | M | decision recorded |
| E7-4 | Recurring dependency check (Dependabot or Renovate) and a Groovy 6 / Spock 2.5 move once a Micronaut release adopts Groovy 6 | P2 | S | PRs opened automatically; `docs/DEPENDENCIES.md` re-checked |

### E8 Platform and quality
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E8-2 | CI on pull requests runs unit + Testcontainers suite and the E1 smoke benchmark with regression threshold | P1 | M | PR fails if msg/s drops > 15% |
| E8-3 | Health endpoint reflects consumer liveness (stream terminated => unhealthy) and Neo4j/Rabbit connectivity | P1 | S | k8s probe test |
| E8-4 | Remove unused `lib/*.jar`, stale `micronaut-cli.yml`, update Kubernetes manifest (image, resources, probes, secrets) | P2 | S | |
| E8-5 | Secrets: no default credentials outside tests; read from env/secret files; fail fast | P1 | S | |
| E8-6 | Stricter schema: `propertyNames`, scalar-only values for dimensions and properties so bad nested values fail validation instead of at Neo4j | P2 | S | rejected at validation with clear message |
| E8-7 | The dev `docker-compose.yml` `rabbitmq` service failed to start on 2026-11-02 (`.erlang.cookie: eacces`, even on a fresh volume) while a plain `docker run` of the same image worked; find the difference and fix or document | P2 | S | `docker compose up` brings RabbitMQ up |

### E9 Modeling flexibility
| ID | Story | P | Size | Acceptance |
|---|---|---|---|---|
| E9-2 | Pluggable surrogate-ID strategy per label: none / `randomUUID()` (string) / `uuid()` UUID type (Enterprise, 2026.08+, Cypher 25) / producer-supplied; documented migration | P1 | M | config per NodeType; works on Community (string) and Enterprise (UUID type) |
| E9-3 | Key evolution: allow a message to change dimension values of an existing node by matching on the surrogate ID (rename without duplicating) | P2 | L | rename test keeps node, relationships and surrogate ID |
| E9-4 | Relationship identity: optional relationship key (e.g. `RelKey`) so parallel same-type relationships between the same two nodes are possible (today MERGE on type+endpoints collapses them) | P1 | M | two transactions between the same accounts yield two relationships, idempotently |
| E9-5 | Secondary labels and multi-label nodes (`Labels: []`) | P2 | S | schema + tests |
| E9-6 | Typed properties beyond dates: duration, point, float vectors, homogeneous lists; type hints in the message | P2 | M | round-trip tests; mixed Long/Double lists normalised |
| E9-7 | Connections of connections / arbitrary graph fragments (message = small subgraph with local ids) instead of star-only | P2 | L | schema v2 + tests |
| E9-8 | Configurable mapping layer (source record + mapping file -> graph) so producers need not emit this JSON; e.g. CSV/Parquet/Kafka adapters | P1 | L | sample mapping ingests CSV without a JSON producer |
| E9-9 | Optional GRAPH TYPE / constraint provisioning from a mapping (Enterprise): node keys, property types, relationship endpoint validation | P2 | M | generated DDL; works as a no-op on Community |
| E9-10b | **Vendor-neutral names, part 2.** Left from E9-10: rename the `com.menome` package (needs a name from the owner), the `menome/message-server` image in `.github/workflows/docker-build.yml` and `Jenkinsfile` (a registry location the owner chooses), the Menome wording in `RESUME.md`, and decide whether the wire vocabulary (`ConformedDimensions`, `NodeType`, `RelType`, `ForwardRel`, `RelProps`) stays or gets an alias mapping (ties to E9-8). Historic benchmark reports keep the old marker names | P2 | S | no `menome` string in `src`, build files, CI or docs outside a legacy note |
