# Running and validating against a Neo4j cluster

`docker-compose.cluster.yml` is a 3-primary Neo4j Enterprise cluster on one machine (evaluation licence: running it means
accepting Neo4j's evaluation terms) for checking the message server against routing, leader election and failover. First
validated 2026-11-02 on `neo4j:2026.10.1-enterprise`.

```
docker compose -f docker-compose.cluster.yml up -d --wait --pull never      # ~25 s; healthy = all 3 report `neo4j` online
NEO4J_HOST=neo4j://localhost NEO4J_BOLT_PORT=7687 <message server or benchmark>   # routing driver finds the leader
RUN_CLUSTER_TESTS=Y ./gradlew test --tests '*ClusterSpecification'                # the automated checks below (~2.5 min)
docker compose -f docker-compose.cluster.yml down -v
```
Which member is the leader (inside a container always pass `-a bolt://localhost:7687`, see below):
```
docker exec cluster-server1 cypher-shell -a bolt://localhost:7687 -u neo4j -p password \
  "SHOW DATABASES YIELD name, address, role, writer WHERE name = 'neo4j' RETURN address, role, writer"
```
Members are `cluster-server1..3`, published on Bolt 7687, 7688, 7689. Use a `neo4j://` host (`NEO4J_HOST=neo4j://localhost`); the
message server's `Neo4J.openDriver` accepts a host that already carries a scheme.

## Things that were wrong or surprising, found by running it
* **Neo4j 2026 renamed the discovery setting.** `dbms.cluster.discovery.endpoints` is *removed and ignored* ("has no effect
  anymore" in the log); it is now `dbms.cluster.endpoints`. With it empty a server silently starts as a standalone DBMS
  (the log shows it resolving only itself), so three containers came up "healthy" as three unrelated databases. The compose
  file now sets `NEO4J_dbms_cluster_endpoints`.
* **Container health must mean "the cluster formed", not "Bolt answers".** The healthcheck requires 3 members to report the
  default database online.
* **Inside a container use a direct `bolt://localhost:7687`.** `cypher-shell`'s default `neo4j://` follows the routing table to the
  advertised `localhost:7688/7689`, which only exist from the host's point of view; the failure looks like an authentication error
  ("Access denied", "scheme 'basic' is not supported") and cost an hour. Each member also now has a separate internal routing
  listener on 7688 (logged as "Bolt (Routing)").
* `CALL dbms.cluster.role('neo4j')` no longer parses; use `SHOW DATABASES ... writer`.
* **Deleting a hub that still has hundreds of thousands of relationships exhausts a member's transaction memory** (`51N72 memory
  pool out of memory`, 717 MB pool on a 1 GB heap): the test hubs carry the test `SourceSystem` tag too, so a blanket
  `MATCH (n) ... DETACH DELETE n` hits them while they are huge, and even small batches then fail. Delete the leaf nodes
  (employees) first, in batches, then the hubs. The end-to-end script does; the benchmark's cleanup (`IN TRANSACTIONS OF 10000
  ROWS`) survives its 150k-message volumes but not much more (E1-6).
* The compose `rabbitmq` service of the dev stack failed to start (`.erlang.cookie: eacces`) while a plain `docker run` of the
  same image worked; not investigated, the end-to-end run below used `docker run`.

## What was checked against the live cluster (`ClusterSpecification`, 7 tests, all passing)
| Check | Result |
|---|---|
| 3 servers, `neo4j` has 3 primaries and 0 secondaries, a routing driver connects | yes |
| A batch through the routing driver: the constraint is created once (on the leader) and every member sees it and the data | yes |
| A write sent straight to a follower | `ClientException Neo.ClientError.Cluster.NotALeader: ... Writes must pass through the leader`. Not a data error: the batch propagates (requeued), nothing is dead-lettered. This is what E5-6 fixed; before it, a failover would have dead-lettered whole batches |
| `KEY_CONSTRAINT=nodekey` | creates a `NODE_KEY` (E3-12) |
| `nodekey` over an existing uniqueness constraint | **was a bug**: Neo4j answers `22N66 conflicting constraint already exists` (a node key and a uniqueness constraint on the same keys cannot coexist, in either direction), the failure was swallowed and the uniqueness constraint stayed. `SchemaManager` now looks at what exists first: a node key satisfies both modes, a uniqueness constraint satisfies `unique`, and on Enterprise an existing uniqueness constraint is swapped for the node key (restored if that fails); on Community it is left alone |
| Leader stopped (SIGTERM) while 20,000 messages are written through 2 writers | all 20,000 written exactly once, no relationship lost or duplicated, 0 bad-data errors, 0 batches had to be redelivered; 12 s |
| Leader killed (SIGKILL) during the same load | same result; 37 s. The driver's own transaction retry (30 s) rode through the roughly 25 s outage (leader failure detection window 20-23 s), so no batch reached the server's requeue path |

Those two did not reach the server's own nack/requeue (the driver's retry absorbed the outage), so the next section does.

## End to end: the real server, RabbitMQ and the cluster (`scripts/cluster-quorum-loss-e2e.sh`)
The server (`neo4j://localhost`, 2 writers) consumes 200,000 messages. After a few batches the leader **and a follower are
killed** (quorum lost) for 55 s, longer than the driver's 30 s transaction retry, then restarted.

| | |
|---|---|
| During the outage | writes stall; the in-flight batches fail with `SessionExpiredException: Failed to obtain connection towards WRITE server`; the server logs "could not be processed, requeueing" for **1-2 batches** (nack + requeue; it varies by run) and the pipeline backs off; RabbitMQ prefetch stays bounded at 2,000 unacknowledged messages |
| After the members return | the stream resumes by itself |
| End state | 200,000 employees, 200,000 distinct, 600,000 relationships, 3 hubs; queue 0 ready / 0 unacknowledged; **0 messages reported as bad data, nothing in the error queue** |

So at-least-once holds through a quorum loss: requeued batches are delivered again and re-applied by the idempotent MERGEs
without a single duplicate. A first attempt with 20,000 messages proved nothing (the server drained them in 6 s, before the
outage began); the stream has to be long enough to still be running when the members are killed.

Note: the driver's 30 s retry means a normal failover (12 s graceful, 37 s killed) mostly never reaches the requeue path.
Whether to raise it so a hard failover never does is E5-8.

## Is a cluster faster? No: it costs 16-45% of write throughput (measured)
Benchmark through the routing driver (150,000 messages, 2026-11-02). All three setups use the pipeline; `r0` = 0 transient retries.
The fair comparison is the last two rows of each block: a single Enterprise instance given exactly one cluster member's
resources (2 CPUs, 3 GB, 1 GB heap, 1 GB page cache) against the 3-member cluster.

| msg/s | Community, 4 CPUs, 2 GB heap | Enterprise single, 2 CPUs | Enterprise 3-node cluster (leader has 2 CPUs) | cluster vs same-resources single |
|---|---|---|---|---|
| **1 writer** | | | | |
| node-only | 52,019 | 35,467 | 29,845 | -16% |
| hubs3 | 20,299 | 17,749 | 12,489 | -30% |
| hub-fanout | 29,441 | 29,193 | 16,353 | -44% |
| wide | 7,567 | 8,251 | 6,084 | -26% |
| unique-dest | 12,709 | 12,439 | 8,381 | -33% |
| **4 writers** | | | | |
| node-only | 118,413 | 87,389 | 71,480 | -18% |
| hubs3 | 30,003 | 32,892 | 19,573 | -40% |
| hub-fanout | 40,363 | 40,071 | 22,136 | -45% |
| wide | 8,572 | 9,622 | 6,832 | -29% |
| unique-dest | 33,856 | 26,292 | 18,591 | -29% |

A write must reach a quorum before it commits, so every commit waits for a network round trip to a follower. The cost is
largest where commits are the larger share of the work (hubs and hub-fanout: -40..-45%) and smallest on node-only. A cluster
is for availability and read capacity, not write throughput.

Side observation, not explained and not a finding yet: a single Enterprise instance with half the CPUs and heap of the
Community container matched it on the hub-bound profiles (hubs3 32.9k vs 30.0k, wide 9.6k vs 8.6k at 4 writers) and was lower
on the profiles that create the most nodes (node-only, unique-dest). The two runs also differ in heap (1 vs 2 GB) and store
format defaults, so nothing should be concluded without a controlled comparison.

Reports: `docs/benchmarks/2026-11-02-cluster-w1.md`, `-cluster-w4.md`, `-enterprise-single-2cpu-w1.md`, `-enterprise-single-2cpu-w4.md`.
