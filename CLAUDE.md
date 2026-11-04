# message-server

Consumes JSON "harvester" messages from RabbitMQ and writes them to Neo4j as nodes and relationships (batched
UNWIND/MERGE). Groovy 5 + Micronaut 5, Java 25 (needs a JDK 25 to build: the Micronaut Gradle plugin refuses to run on 21; versions in `docs/DEPENDENCIES.md`). Goal of the current phase: maximum import throughput without losing
correctness. Read `docs/THROUGHPUT.md` for the analysis and `docs/KANBAN.md` for planned work; `RESUME.md` has the
current state.

## Commands
- Build / fat jar: `./gradlew assemble shadowJar` (jar: `build/libs/messageServer-1.0-all.jar`)
- Unit-only specs (no Docker): `./gradlew test --tests '*Neo4JNodeSpecification' --tests '*MessageParserSpecification'`
- Full suite: `./gradlew test` - needs Docker (Testcontainers starts RabbitMQ and `neo4j:2026.10.1`); ~2 min.
  `RUN_WITH_TEST_CONTAINERS=N` uses services you already run, configured through the env vars below.
- One spec: `./gradlew test --tests '*MessageBatchProcessorSpecification'` (tests fork per class: `forkEvery = 1`).
- Local stack: `./gradlew shadowJar && docker compose up --build`
- Throughput benchmark (needs Docker): see `docs/benchmarks/README.md`; compare only runs of >=150k messages on the pinned
  `docker-compose.benchmark.yml`. Stage metrics: `docs/METRICS.md`. Never run two `./gradlew test` at once (port 8081).
- Skipped by default (need external infra): the 500k/200k/100k Symend/ODS specs in `MessageServerCommandSpecification`.

## Architecture (src/main/groovy/com/menome)
- `MessageServerCommand` - entry point. RabbitMQ `consumeManualAck` -> `bufferTimeout(batch, 2s)` ->
  `MessageBatchProcessor.process` -> ack on commit, nack+requeue on infrastructure error, bad messages to
  `<queue>.errors` (`util/ErrorPublisher`).
- `messageParser/ConvertJSonToNeo4JObjects` - JSON -> `Neo4JNode` / `Neo4JConnection` (Gson, `LONG_OR_DOUBLE`).
- `message/Neo4JNode`, `Neo4JConnection` - own their Cypher. **`signature()` is the central idea**: nodes (or
  connections) with the same signature (label, sorted dimension keys, property keys, flags) share one statement and
  are written in one `UNWIND $parms` batch. Never build one statement from the "first" element of a mixed group.
- `messageBatchProcessor/MessageBatchProcessor` - validate (everit JSON schema, `harvester_schema.json`), group by
  signature, merge nodes -> connect -> delete relationships -> delete nodes; bisect on data errors only.
- `util/Cypher.id()` - **always** use it for labels, property keys and relationship types interpolated into Cypher.
- `util/PreferenceType` + `ApplicationConfiguration` - all config; env var > JVM property > default.

## Rules that are easy to break
- Data error vs infrastructure error: an allow-list. Only `ClientException`s with code `Neo.ClientError.Statement.*` or
  `Schema.*` (plus the driver's own "Unable to convert" / "Unsupported parameters type") are bisected and reported per
  message. Everything else - cluster errors such as `NotALeader`, read-only/unavailable database, security, timeouts,
  codes nobody classified - must propagate so the batch is requeued. Writes are idempotent MERGE so reprocessing is safe;
  keep it that way. Never go back to a deny-list: a failover would dead-letter whole batches.
- `MessageBatchProcessor.process` = `parse` (CPU only) + `write` (database); `BatchPipeline` runs them on different threads
  (`PARSER_THREADS`, `WRITER_THREADS`, default 1/1) and is what both the server and the benchmark use. With 1 writer batches
  keep arrival order; with more, two batches carrying the same node can commit in either order. Never add shared mutable
  state to parse or write (several run at once); schema DDL is serialised in `SchemaManager`.
- Lock order is a correctness property once writers run in parallel: nodes and relationship statements are ordered by
  label (relationships by the label they point at), rows by key (`LockOrder`), in both phases and in every batch. Break it
  and writers deadlock (measured: 1.5k instead of 8k msg/s). Groups of one label keep first-seen order so "last message wins"
  survives. Do not reorder statements without re-running the writer curve in `docs/benchmarks/README.md`.
- A batch is written in at most 3 write transactions (all node merges, all relationship statements, all node deletes),
  not one per signature. On a data error the phase rolls back and is re-run one statement at a time, then bisected
  (`executePhase`). Keep statement order inside a phase (merges before deletes).
- Messages may not set the properties the server manages: the id and created-at properties (`Uuid`/`CreatedAt` by default; names are config: `ID_PROPERTY`, `CREATED_AT_PROPERTY`, `none` = off; `TheLinkAddedDate` is the legacy name), and Name/Label/Priority/SourceSystem
  when the message has that field itself (`Neo4JNode.assertNoReservedProperties`). `SourceSystem` in a connection's
  `Properties` is legitimate (a connected node has no such field).
- `Neo4JNode`/`Neo4JConnection` cache `signature()`, `identity()` and `dimensionKeys()`: never change a node after it has
  been grouped.
- Failure accounting is by message index in the batch (`sourceIndex`), not by string identity - identical messages are legal.
- Within one batch the last message for the same node wins (`identity()` = dimension values).
- Messages may carry `DeleteNode`, `DeleteProperties`, `RelProps`, `DateProperties`, ...: see schema; covered by tests.
- Counters: `successCount = messages - failed messages`. Don't reintroduce `messages.size()`.
- Whole numbers must stay `Long`, or integer keys stop matching existing nodes.
- Test endpoints (`/GenerateTestMessages`, `/DeleteTestMessages`) are POST and need `ENABLE_TEST_ENDPOINTS=Y`.
- Every statement sent to Neo4j goes through `Cypher.versioned()` (pins `CYPHER 25`; a database with no
  `db.query.default_language` falls back to Cypher 5, and `DISJOINT BY` needs Cypher 25). Don't bypass it.
- Neo4j 5+ syntax only: `CREATE CONSTRAINT IF NOT EXISTS FOR ...`, `randomUUID()` (no APOC).
- Cluster facts verified 2026-11-02 (`docs/CLUSTER.md`, `RUN_CLUSTER_TESTS=Y ./gradlew test --tests '*ClusterSpecification'` with
  `docker-compose.cluster.yml` up): `NotALeader` is a `ClientException` and must requeue; Neo4j 2026 uses `dbms.cluster.endpoints`
  (the old discovery setting is silently ignored and servers start standalone); a node key and a uniqueness constraint on the same
  keys conflict in both directions (`SchemaManager` checks what exists first); inside a container use `bolt://localhost:7687`, never
  the default `neo4j://`.
- A node type is merged on its dimension keys, which carry a uniqueness constraint (`SchemaManager`, `KEY_CONSTRAINT`):
  that is what makes concurrent writers safe (without it 4 writers made 412 nodes for 300 ids). A plain index from an
  older version is dropped to make room; existing duplicates fall back to a plain index plus an ERROR log. Neo4j throws
  a failed constraint creation as a *database* error, not a client error: catch `Neo4jException`, and test with a fake
  that throws what the server really throws.

## Conventions
- Groovy is dynamic: a compile pass does not prove correctness. Run the Spock specs; add a failing spec first for bugs.
- Match surrounding style; comments explain why. Spock `then:` blocks must use real conditions - a closure returning
  a boolean inside `with {}` asserts nothing.
- Don't implement items from `docs/KANBAN.md` Backlog unless asked; move the story when you start/finish it.
- Dependency versions live in `build.gradle` / `gradle.properties`; Micronaut BOM manages most. Prefer the BOM version.
- Never commit credentials; defaults in `PreferenceType` are for local/test only (E8-5).

## Config (env vars)
RABBITMQ_HOST/PORT/USER/PASSWORD/QUEUE/ERROR_QUEUE/EXCHANGE/EXCHANGE_TYPE/ROUTE/BATCH_SIZE, NEO4J_HOST/BOLT_PORT/USER/PASSWORD,
KEY_CONSTRAINT (unique|nodekey|index), ID_PROPERTY, CREATED_AT_PROPERTY, WRITER_THREADS, PARSER_THREADS, RELATIONSHIP_TRANSACTIONS (phase|statement),
HTTP_SERVER_PORT, ENABLE_METRICS (Y), ENABLE_TEST_ENDPOINTS (Y), CASE_STYLE (camel|lower|upper),
SHOW_CONNECTION_LOG_OUTPUT, RUN_WITH_TEST_CONTAINERS. Defaults in `util/PreferenceType.groovy`.
