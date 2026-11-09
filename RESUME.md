# RESUME - where this project stands

Last updated: 2026-11-09 (everything pushed; see "2026-11-09 update" at the end). Read this first when picking the work back up.

## The goal
Take the original Menome data atomization pattern (harvesters emit atomised JSON messages; the refinery, this message
server, merges them into a knowledge graph, continuously) to current code and methods, then optimise ingestion to the limit,
ideally beyond anything else available. Three pillars, scored below. Planned work: `docs/KANBAN.md`.

## Scorecard (honest)
| Pillar | Status | Evidence | What is missing |
|---|---|---|---|
| **1. Current code and platform** | Done (2026-11-02) | Java 25, Gradle 9.8.2, Micronaut 5.2.4, Groovy 5.0.8, Neo4j driver 6.3.0, Neo4j 2026.10.1, Cypher 25 pinned on every statement, Testcontainers 2.0.5; every library is the newest release as of 2026-11-02 (`docs/DEPENDENCIES.md`); Card pattern, Redis and the embedded Neo4j removed; no APOC | Groovy 6 / Spock 2.5 are out but the Micronaut 5.2.4 BOM is on Groovy 5.0.8, so deferred on purpose; no Dependabot/Renovate; stale `lib/*.jar` and Kubernetes manifest (E8-4); default credentials (E8-5) |
| **2. Current methods and correctness** | Done for the write path, open around it | Idempotent batched MERGE, at-least-once with manual acks, failure allow-list (a failover requeues, never dead-letters), uniqueness or node-key constraints, reserved-property protection, stage metrics, benchmark harness, 206 tests green, **validated on a real 3-node Enterprise cluster** incl. quorum loss with the real server | CI with a regression gate (E8-2), health reflects consumer liveness (E8-3), graceful drain on SIGTERM (E4-5), dead-letter replay (E6-4), harvester-side modernisation / mapping layer (E9-8) |
| **3. Maximum ingestion performance** | **Improved, not proven best** | Table below | **No valid ceiling yet** (E10-1: the replay harness came out *slower* than the server, unexplained), no comparison with any other tool, no server-class hardware, no soak (E10-2..E10-4) |

**On "beyond anything else available":** nothing in this repository supports that claim yet. We know we are 1.4-4.1x faster
than the old code on one laptop. We do not know how close we are to what the database can take (E10-1), how we compare with
the other live-ingestion patterns, the Neo4j Kafka connector and the Spark connector (E10-2), or what a server with real disks and
a network does (E10-3). Until E10-1 and E10-2 exist, say "faster than the previous version", not "faster than X".
Decided 2026-11-02: the comparables are **Kafka and Spark**, because they are live ingestion like this server. `neo4j-admin import`
is **not** a competitor (offline, file-level, bypasses transactions and logs); it stays only as the initial-load path (E7-1).
Comparisons must be on a comparable base (E10-2 rules): same database container and settings, same data and verified same graph,
durability on, the same total CPU/memory budget for each tool's whole pipeline, run on the same hardware (E10-3).

## Performance, measured (150k messages, pinned Community container 4 CPUs / 2 GB heap on Docker Desktop, msg/s)
| Profile | Old code | Now, default (1 writer) | Now, 4 writers | Default gain | 4-writer gain |
|---|---|---|---|---|---|
| hubs3 (the sample message) | 10,979 | 20,299 | 30,003 | 1.8x | 2.7x |
| unique-dest | 8,203 | 12,709 | 33,856 | 1.5x | 4.1x |
| node-only | 34,469 | 52,019 | 118,413 | 1.5x | 3.4x |
| hub-fanout | 20,682 | 29,441 | 40,363 | 1.4x | 2.0x |
| wide (41 properties) | 5,244 | 7,567 | 8,572 | 1.4x | 1.6x |

Read these with the caveats: laptop; client, broker and database share the machine; the benchmark feeds the pipeline from
memory, not through RabbitMQ; **the default is 1 writer** (`WRITER_THREADS=1`), so the left gain column is what a deployment
gets unless someone changes it. The hub profiles do not scale with writers (efficiency at 4 writers 37% hubs3, 34% hub-fanout,
28% wide, 67% unique-dest): a relationship write-locks both ends until commit. Where the gains came from: E2 cached signatures and
`@CompileStatic` (+6..16%), E3-2 one transaction per phase (2 commits per batch instead of 7), E4-1 parse overlaps write
(+14..37%), E4-2 parallel writers with one global lock order. Cost: the uniqueness constraint takes 3-13% on create-heavy profiles.
A cluster costs a further 16-45% of write throughput (quorum commits): it is for availability, not speed.

## What E10-1 found so far (2026-11-02; `docs/benchmarks/README.md`, last section)
* `./gradlew ceiling` replays the server's statements through raw sessions; the graph is checked identical to the server's. But the
  **real server beat that replay in all 10 interleaved cases (104-135%)**, so it is a lower bound, not a ceiling, and the cause is
  unexplained (cold store, heap, session reuse ruled out; memory locality of the pre-built rows is the untested guess). No
  "% of ceiling" is claimed anywhere.
* **Batch 2000 instead of 500 is a measured win in the real server**: +16..46% on node-only, hubs3, hub-fanout, nothing on wide or
  unique-dest; costs latency (p99 up to 5x) and memory. Default unchanged; decision is E3-14.
* Hub profiles saturate near 38k (hubs3), 45k (hub-fanout), 8k (wide) however many sessions: the statement shape is the limit.
* Java 21 -> 25 and Micronaut 4 -> 5 did not slow the hub profiles. node-only at 1 writer reads 43-46k against 52k on 11-01 (and
  141-156k against 118k at 4 writers): the noisiest profile, unresolved.

## Where the remaining speed probably is (hypotheses)
0. **`BATCH_SIZE` 2000** (E3-14): measured, see above; the cheapest gain on the board.
1. **Make more than one writer safe by default** (E4-3 key-aware routing): the gain from 1 to 4 writers is 1.1-3.9x, but two
   batches carrying the same node can then commit out of order, so the default stays 1.
2. **Hubs** (E3-7 remainder, E3-8 super-node strategy, E3-9 initial-load `CREATE`): hub profiles are the sample message's shape.
3. **Server-side batching and parallelism** (E3-3, E3-4, E3-6, E3-13 `DISJOINT BY`): fewer round trips, error isolation inside the
   database. Untested whether it beats application-level writers.
4. **App CPU** (E2-2 parse once, E2-4/5/6): matters once the database is no longer the limit, which on node-only already nearly holds.
5. **Broker** (E6-1 prefetch knee, E6-2 sharded queues): unmeasured; the benchmark bypasses RabbitMQ (E10-4).
6. **Offline bulk path** (E7-1 `neo4j-admin import`) for initial loads: the fastest way to load a large first batch by design, but not a like-for-like comparison with this server.

## State of the repository
All commits are pushed to `origin/main` (github.com/menome/message-server) as of 2026-11-09, working tree clean. The three oldest were squashed on 2026-11-05 into
`69be69c` because they did not compile alone (E8-8); the others are `4e20a01` benchmark, specs, environments, docs; `2e02f98` cluster validation and
node-key upgrade; `f571db6` platform upgrade; `18c5784` E10-1 replay; `5789ebd` README and board; `13b10c5` E3-14 matrix; `de3c192` E3-14 default 2000
and E9-10 part 1.

Suite: **211 tests, 0 failures, 10 skipped** (full run 2026-11-04 on JDK 25, including `CeilingRunnerSpecification`'s 7) (4 need external infrastructure; the rest are the cluster specs, run only with
`RUN_CLUSTER_TESTS=Y`). No containers are running.

## Cluster validation, 2026-11-02 (details and tables: `docs/CLUSTER.md`)
* The compose file was wrong for Neo4j 2026: `dbms.cluster.discovery.endpoints` is silently ignored (now `dbms.cluster.endpoints`),
  so three "healthy" containers were three standalone databases. Fixed; healthy now means "all 3 members online".
* `NotALeader` is a `ClientException` and is requeued (E5-6 confirmed live). Leader stopped (12 s) or killed (37 s) under load:
  nothing lost or duplicated. **Quorum lost for 55 s with the real server and RabbitMQ: 1-2 batches requeued, stream resumed by itself,
  200,000 employees exactly once, queue empty, nothing dead-lettered** (`scripts/cluster-quorum-loss-e2e.sh` prints `RESULT: PASS`).
* Bug found and fixed: a node key and a uniqueness constraint on the same keys conflict in both directions; `nodekey` over an
  existing constraint did nothing, silently. `SchemaManager` now checks what exists first.

## Decisions waiting for a human
1. **Default for `WRITER_THREADS`** (now 1, keeps batch order; more is up to 3.9x faster on messages that do not share nodes but a node
   in two batches can commit in either order). Leave at 1, set 4 where order across batches does not matter, or build E4-3 first.
2. ~~Commit history~~ done 2026-11-05 (E8-8): squashed and pushed.
3. **E10-1 next step:** find out why the replay is slower than the server (feed rows from a producer thread rather than pre-built memory), or define the ceiling another way. (E3-14 is decided: 2000.)
4. **Retry window** (E5-8): the driver rides through 30 s, a killed leader takes 37 s; longer outages requeue, which is verified safe.
5. E7-3 (dependency audit, "latest drivers for the whole stack") is defined, not started.

## Known limits (not bugs)
* Hub-bound messages scale poorly with writers (above). E3-7 acceptance (60% efficiency at 4 writers) is **not met** for hub profiles.
* E2-1 grouping under 5 us per message holds for 3 of 5 profiles (wide 8.0, unique-dest 5.7): E2-6.
* Community exposes no server metrics over Bolt, so page cache ratio and lock waits are not recorded (E1-5).
* Cleanup that `DETACH DELETE`s a hub with hundreds of thousands of relationships exhausts a 1 GB-heap cluster member (E1-6).
* The dev `docker-compose.yml` RabbitMQ service failed to start on 2026-11-02 (E8-7); a plain `docker run` works.
* Enterprise single-instance (2 CPUs, 1 GB heap) matched Community (4 CPUs, 2 GB) on hub profiles; unexplained (E1-7).
* Behaviour changes to know before deploying are in the README "Upgrade notes".

## How to work here
* **Builds need JDK 25** (the Micronaut Gradle plugin refuses to run on 21). It is installed keg-only: `export
  JAVA_HOME=/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home` before `./gradlew`.
* Ceiling: `NEO4J_BOLT_PORT=7688 ./gradlew ceiling --args="--container=neo4j-bench --messages=150000 --warmup=10 --sessions=1,4 --batches=500 --compare=1,4 --repeats=3 --label=x --out=docs/benchmarks"`.
* Benchmarks: `docs/benchmarks/README.md`. Pinned database: `docker compose -f docker-compose.benchmark.yml up -d --wait
  --pull never`, `NEO4J_BOLT_PORT=7688 ./gradlew benchmark --args="--container=neo4j-bench --messages=150000 --warmup=10
  --writers=1 --label=x --out=docs/benchmarks"`. Compare only runs of >=150k messages on a warm container; +-5% is noise.
* The cluster: `docker compose -f docker-compose.cluster.yml up -d --wait --pull never`, `RUN_CLUSTER_TESTS=Y ./gradlew test --tests
  '*ClusterSpecification'`, `scripts/cluster-quorum-loss-e2e.sh`; inside a container always `cypher-shell -a bolt://localhost:7687`.
  Tear down with `down -v`.
* **Never run two `./gradlew test` at once** (both start a server on port 8081).
* The kanban lives in `docs/KANBAN.md` only (standing decision for all git projects); do not set up the Neo4j kanban plugin.
* Quick verification:
```
./gradlew test --tests '*Neo4JNodeSpecification' --tests '*MessageParserSpecification' --tests '*PhaseTransactionSpecification' \
  --tests '*BatchPipelineSpecification' --tests '*LockOrderingSpecification'    # seconds, no Docker
./gradlew test                                                                  # ~3 min, needs Docker
RUN_CLUSTER_TESTS=Y ./gradlew test --tests '*ClusterSpecification'              # ~2.5 min, needs the cluster up
```

## 2026-11-04 update
- E3-14 done: `RABBITMQ_BATCH_SIZE` default is now 2000 (`docs/benchmarks/README.md`, "Batch size through the real server" and "remaining checks").
- E9-10 part 1 done: `ID_PROPERTY` / `CREATED_AT_PROPERTY` are config; old graphs: `CREATED_AT_PROPERTY=TheLinkAddedDate`. Part 2 (package name, image name, wire vocabulary) is E9-10b and needs decisions from the owner.
- Suite: 211 tests, 0 failures, 10 skipped on JDK 25. Gotcha: a spec that wipes JVM system properties (`System.setProperties(new Properties())`) hangs the Gradle test worker when anything fails. Don't.
- New benchmark tool: `./gradlew bisect` (`BisectCost`): cost of one bad message and message sizes.

## 2026-11-09 update (reboot point)
- **Pushed:** `origin/main` = local `main`, working tree clean, no other branches. The three oldest commits were squashed (E8-8) and the result built in a clean worktree. Suite last run 2026-11-04: 211 tests, 0 failures, 10 skipped (JDK 25).
- **Owner's intent for the repo:** open source, used to illustrate the harvest/refinery pattern. If it resonates it moves to Neo4j Labs; otherwise it is paused as is. Before a Labs move, do E9-10b (package name, `menome/message-server` image in CI and `Jenkinsfile`, wire vocabulary alias or keep).
- **To resume after a reboot:** Docker Desktop must be running (tests use Testcontainers; benchmarks use `docker-compose.benchmark.yml`, port 7688). Build with `JAVA_HOME=/opt/homebrew/opt/openjdk@25/libexec/openjdk.jdk/Contents/Home`. No containers or background jobs are left running. Never run two `./gradlew test` at once.
- **Not demonstrated:** "faster than anything else". No Kafka or Spark comparison (E10-2) exists, and the E10-1 replay is a lower bound, not a ceiling (the server ran at 104-135% of it, cause unexplained).
- **Open and unmeasured:** the 2 s `bufferTimeout` (reasoned, not measured, not configurable); node-only at 1 writer is noisy (~25% run to run); `WRITER_THREADS` default (1 vs 4).
