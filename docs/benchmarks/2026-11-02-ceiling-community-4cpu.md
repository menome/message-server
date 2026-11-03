# Ceiling ceiling-community-4cpu - 2026-11-02

## Environment
- Neo4j: 2026.10.1 community at localhost:7688
- Container `neo4j-bench`: image `neo4j:2026.10.1@sha256:86f8e01402fd0d5fd23e8968375fefdc21284a5c1751fa3fa3de8091a08b6dd6` (id sha256:86f8e01402fd0d5fd23e8968375fefdc21284a5c1751fa3fa3de8091a08b6dd6, digests ["neo4j@sha256:86f8e01402fd0d5fd23e8968375fefdc21284a5c1751fa3fa3de8091a08b6dd6"])
- Container limits: CPUs 4.000000000, memory 8 GB
- Docker host VM: 8 CPUs, 17009807360 bytes memory, Docker 29.9.2
- Neo4j settings in effect (not set = server default):
  - `server.memory.heap.initial_size` = 2.00GiB
  - `server.memory.heap.max_size` = 2.00GiB
  - `server.memory.pagecache.size` = 2.00GiB
  - `db.tx_log.rotation.size` = 256.00MiB
  - `db.tx_log.rotation.retention_policy` = 2 days 2G
  - `db.checkpoint` = PERIODIC
  - `db.checkpoint.interval.time` = 15m
  - `db.checkpoint.interval.tx` = 100000
  - `db.checkpoint.iops.limit` = 600
  - `db.query.default_language` = CYPHER_25
  - `dbms.cypher.transactions.default_subquery_batch_strategy` = DEFAULT
- Client: Java 25.0.5.0.3, 16 cores, 48 GB RAM, max heap 12288 MB, Mac OS X 26.7.1 aarch64
- Cypher: CYPHER 25
- Ceiling run: 150000 messages per profile, batch sizes [500, 2000, 10000], sessions [1, 2, 4, 8], first 10 batches are warm-up and not measured, seed 42

## What this measures

The server's own statements and rows, in the server's lock order, replayed by N raw driver sessions (one `executeWrite` per phase) from batches that were parsed, grouped, ordered and parameterised beforehand. Only the database work is timed: no parsing, no pipeline, no stage timing, no failure handling, no RabbitMQ. It is the most this database takes for this write shape on this hardware; `server % of ceiling` is how much of it the message server uses.

## Graph equality with the server

| Profile | same nodes and relationships as the server wrote (SHA-256 of label+properties of every tagged node and relationship, `Uuid`/`TheLinkAddedDate` excluded) |
|---|---|
| node-only | yes (2000 messages) |
| hubs3 | yes (2000 messages) |
| hub-fanout | yes (2000 messages) |
| wide | yes (2000 messages) |
| unique-dest | yes (2000 messages) |

## Ceiling, msg/s

| Profile | batch | 1 session | 2 sessions | 4 sessions | 8 sessions | retries |
|---|---|---|---|---|---|---|
| node-only | 500 | 48,063 | 91,300 | 162,511 | 201,793 | 0 |
| node-only | 2000 | 52,671 | 87,959 | 127,411 | 187,425 | 0 |
| node-only | 10000 | 61,151 | 84,448 | 106,404 | 175,645 | 0 |
| hubs3 | 500 | 19,227 | 28,180 | 30,523 | 29,712 | 0 |
| hubs3 | 2000 | 20,843 | 32,409 | 38,115 | 36,638 | 0 |
| hubs3 | 10000 | 25,093 | 33,327 | 33,672 | 32,369 | 0 |
| hub-fanout | 500 | 27,507 | 30,599 | 32,873 | 34,256 | 0 |
| hub-fanout | 2000 | 33,534 | 41,354 | 43,899 | 45,382 | 0 |
| hub-fanout | 10000 | 39,615 | 40,713 | 45,253 | 45,580 | 0 |
| wide | 500 | 6,286 | 7,011 | 6,540 | 6,820 | 0 |
| wide | 2000 | 7,384 | 7,854 | 8,001 | 7,790 | 0 |
| wide | 10000 | 6,383 | 7,441 | 7,550 | 8,714 | 0 |
| unique-dest | 500 | 10,424 | 18,966 | 33,447 | 41,812 | 0 |
| unique-dest | 2000 | 10,601 | 20,273 | 30,501 | 37,142 | 0 |
| unique-dest | 10000 | 11,904 | 14,730 | 17,205 | 20,481 | 0 |

## What the application does around the database, per message (microseconds, single thread)

| Profile | batch | parse+group | lock ordering | parameter maps | app total | single-thread app ceiling msg/s |
|---|---|---|---|---|---|---|
| node-only | 500 | 5.3 | 0.2 | 0.4 | 5.9 | 168,095 |
| node-only | 2000 | 5.1 | 0.1 | 0.4 | 5.6 | 178,333 |
| node-only | 10000 | 4.9 | 0.3 | 0.5 | 5.7 | 176,543 |
| hubs3 | 500 | 16.9 | 1.6 | 1.5 | 20.0 | 50,080 |
| hubs3 | 2000 | 17.5 | 2.1 | 2.1 | 21.7 | 46,143 |
| hubs3 | 10000 | 16.7 | 2.3 | 2.5 | 21.4 | 46,720 |
| hub-fanout | 500 | 8.5 | 0.2 | 0.6 | 9.4 | 106,758 |
| hub-fanout | 2000 | 8.5 | 0.4 | 0.9 | 9.8 | 102,393 |
| hub-fanout | 10000 | 8.8 | 0.7 | 1.3 | 10.8 | 92,598 |
| wide | 500 | 34.7 | 2.9 | 3.6 | 41.2 | 24,267 |
| wide | 2000 | 33.2 | 3.8 | 4.5 | 41.5 | 24,114 |
| wide | 10000 | 37.2 | 4.8 | 5.6 | 47.6 | 20,991 |
| unique-dest | 500 | 27.0 | 1.4 | 3.8 | 32.2 | 31,070 |
| unique-dest | 2000 | 29.2 | 2.1 | 5.7 | 37.0 | 27,026 |
| unique-dest | 10000 | 29.6 | 2.5 | 5.9 | 38.0 | 26,287 |

The server runs the three on its parser and writer threads, so the app side only limits throughput when its single-thread ceiling (or P parsers' worth of it) is below the database ceiling for the same number of sessions. Not included: schema checks per batch, session handling, metrics, RabbitMQ read and ack.
