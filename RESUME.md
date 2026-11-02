# RESUME - where this project stands

Last updated: 2026-11-01 (end of the throughput session). Read this first when picking the work back up.

## State in one paragraph
The message server (RabbitMQ -> Neo4j, Groovy 4 / Micronaut 4, Java 21) is correct, tested and now much faster. The
code-review fixes, the dependency upgrade and eight throughput stories are done. Suite: **186 tests, 0 failures, 4 skipped**
(the skipped ones need external infrastructure). Nothing is running (no containers), and **nothing is committed since
`941d7e8`: 63 files are changed or new in the working tree.** Planned work is in `docs/KANBAN.md` (29 done, 46 in Backlog,
one P0 left: E3-7, partly done). Measured results are in `docs/benchmarks/README.md`; the guide for working in the code is
`CLAUDE.md`.

## What was done on 2026-11-01 (all measured on the pinned Community container, 150k messages)
| Profile | Baseline | Now, 1 writer | Now, 4 writers |
|---|---|---|---|
| hubs3 (the sample message) | 10,979 msg/s | 20,299 | 30,003 |
| unique-dest | 8,203 | 12,709 | 33,856 |
| node-only | 34,469 | 52,019 | 118,413 |
| hub-fanout | 20,682 | 29,441 | 40,363 |
| wide (41 properties) | 5,244 | 7,567 | 8,572 |

* **E1** benchmark harness (`./gradlew benchmark`), per-stage metrics (`docs/METRICS.md`), pinned environment, plans.
* **E2-1, E2-3** cached signatures, `@CompileStatic` on the hot path, environment lookups cached.
* **E3-1** uniqueness constraints instead of plain indexes (`SchemaManager`, `KEY_CONSTRAINT`): no duplicates under concurrent writers.
* **E3-2** one write transaction per phase: 2 commits per batch instead of 7.
* **E4-1, E4-2** `BatchPipeline`: the next batch is parsed while one is written; `WRITER_THREADS` writers in parallel with one
  global lock order (without it writers deadlocked).
* **Bug fixes** E5-6 (a failover must requeue, never dead-letter: data errors are an allow-list), E9-1 (messages cannot overwrite
  `Uuid`/`TheLinkAddedDate`/their own Name etc.), and `ENABLE_METRICS=Y` never having worked.
* Behaviour changes to know before deploying are in the README "Upgrade notes".

## Decisions waiting for a human
1. **Default for `WRITER_THREADS`.** It is 1, which keeps batches in arrival order (the last message for a node wins). More
   writers are up to 3.9x faster on messages that do not share nodes, but two batches carrying the same node can then commit in
   either order. Options: leave at 1, set 4 where order across batches does not matter, or build E4-3 (key-aware routing) first.
2. **Commit the work.** 63 files, no commits since `941d7e8`. Suggested split: dependency upgrade and Card/Redis removal;
   correctness fixes (E5-6, E9-1, schema/constraints); throughput (E2, E3-2, E4); benchmark harness and docs. Not done because
   committing was not asked for.
3. **The Enterprise cluster has never been run.** `docker-compose.cluster.yml` (3 primaries, optional reader, evaluation
   licence) is written and validates, but Docker Desktop could not pull `neo4j:2026.10.1-enterprise`: its daemon goes through the
   internal proxy `http.docker.internal:3128` and gets "TLS handshake timeout" while the shell reaches Docker Hub directly (the
   network in use on 2026-11-01 may block large pulls). Try on another network, or check Docker Desktop > Settings >
   Resources > Proxies, restart it, then `docker pull neo4j:2026.10.1-enterprise` and follow the header of the compose file.
   Waiting on it: the live failover check (E5-3), NODE KEY verification (E3-12), Enterprise numbers (E1-5). Expect a cluster to
   be no faster for writes than one instance (one leader, quorum commit); its value is availability and reads.
4. E7-3, a dependency currency audit ("latest drivers for the whole stack"), is defined but not started.

## Known limits (not bugs)
* Messages that all point at the same few nodes (hubs) scale poorly with writers: a relationship write-locks both ends until
  commit. E3-7 acceptance (60% efficiency at 4 writers) is not met for hub profiles: 37% hubs3, 34% hub-fanout, 28% wide, 67% unique-dest.
* The uniqueness constraint costs a little on every node create: -8..-13% on the most create-heavy profile.
* E2-1 grouping under 5 us per message holds for 3 of 5 profiles (wide 8.0, unique-dest 5.7): E2-6.
* Community exposes no server metrics over Bolt, so server-side page cache ratio and lock waits are not recorded (E1-5).

## Next candidates (P0/P1 first)
E4-3 key-aware routing (unlocks more than one writer safely) - E3-7 remainder with E3-8 / E3-9 (hub cost) - E3-13 `DISJOINT BY`
evaluation - E3-3 / E3-4 server-side batching and error isolation - E5-3 live failover (needs the cluster) - E8-2 CI with a
regression gate (needs runs of at least 150k messages, +-5% is noise) - E6-4 dead-letter replay tool.

## How to work here
* Benchmarks: `docs/benchmarks/README.md`. Pinned database: `docker compose -f docker-compose.benchmark.yml up -d --wait
  --pull never` (the image is cached locally), `NEO4J_BOLT_PORT=7688 ./gradlew benchmark --args="--container=neo4j-bench
  --messages=150000 --warmup=10 --writers=1 --label=x --out=docs/benchmarks"`. Compare only runs of >=150k messages on a warm
  container; +-5% is noise.
* **Never run two `./gradlew test` at once** (both start a server on port 8081).
* The kanban lives in `docs/KANBAN.md` only (standing decision for all git projects); do not set up the Neo4j kanban plugin.
* Unused `lib/*.jar`, `micronaut-cli.yml` and `kubernetes/message-server-deploy.yml` (hard-coded IPs, third-party image) are
  left untouched (E8-4).

## How to verify quickly
```
./gradlew test --tests '*Neo4JNodeSpecification' --tests '*MessageParserSpecification' --tests '*PhaseTransactionSpecification' \
  --tests '*BatchPipelineSpecification' --tests '*LockOrderingSpecification'    # seconds, no Docker
./gradlew test                                                                  # ~3 min, needs Docker
```
