# Ceiling ceiling-vs-server - 2026-11-02

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
- Ceiling run: 150000 messages per profile, batch sizes [500], sessions [1, 4], first 10 batches are warm-up and not measured, seed 42

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

## The server against its ceiling (interleaved, 3 repeats, batch 500)

Each repeat runs the ceiling and then the real server pipeline (1 parser, N writers) on the same workload, one after the other, so machine state affects both alike. msg/s as median [min-max]; the last column is the median server over the median ceiling.

| Profile | N | ceiling (N raw sessions) | server (N writers) | server % of ceiling |
|---|---|---|---|---|
| node-only | 1 | 41,197 [40,291-46,224] | 42,932 [36,114-43,410] | 104% |
| node-only | 4 | 140,520 [139,991-147,816] | 155,926 [136,779-159,427] | 111% |
| hubs3 | 1 | 18,121 [18,002-18,574] | 22,504 [22,281-22,619] | 124% |
| hubs3 | 4 | 29,892 [29,170-30,150] | 31,350 [30,658-32,310] | 105% |
| hub-fanout | 1 | 26,347 [25,763-27,024] | 31,191 [31,083-32,313] | 118% |
| hub-fanout | 4 | 32,061 [31,924-32,146] | 41,932 [40,728-42,121] | 131% |
| wide | 1 | 5,770 [5,689-5,980] | 7,788 [7,559-7,841] | 135% |
| wide | 4 | 6,856 [6,796-6,955] | 8,787 [8,569-8,943] | 128% |
| unique-dest | 1 | 10,112 [10,009-10,477] | 12,527 [10,733-12,556] | 124% |
| unique-dest | 4 | 36,455 [35,739-41,342] | 38,965 [37,995-40,195] | 107% |

## Ceiling, msg/s

| Profile | batch | 1 session | 4 sessions | retries |
|---|---|---|---|---|
| node-only | 500 | 45,728 | 141,014 | 0 |
| hubs3 | 500 | 17,747 | 29,444 | 0 |
| hub-fanout | 500 | 25,639 | 31,884 | 0 |
| wide | 500 | 5,835 | 6,842 | 0 |
| unique-dest | 500 | 10,068 | 35,842 | 0 |

## What the application does around the database, per message (microseconds, single thread)

| Profile | batch | parse+group | lock ordering | parameter maps | app total | single-thread app ceiling msg/s |
|---|---|---|---|---|---|---|
| node-only | 500 | 5.2 | 0.1 | 0.4 | 5.7 | 176,436 |
| hubs3 | 500 | 17.6 | 1.5 | 1.4 | 20.4 | 49,053 |
| hub-fanout | 500 | 9.2 | 0.2 | 0.6 | 10.0 | 100,124 |
| wide | 500 | 33.2 | 2.7 | 3.2 | 39.1 | 25,563 |
| unique-dest | 500 | 17.9 | 0.8 | 1.9 | 20.6 | 48,483 |

The server runs the three on its parser and writer threads, so the app side only limits throughput when its single-thread ceiling (or P parsers' worth of it) is below the database ceiling for the same number of sessions. Not included: schema checks per batch, session handling, metrics, RabbitMQ read and ack.
