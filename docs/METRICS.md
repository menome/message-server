# Metrics

Enable with `ENABLE_METRICS=Y`; scrape `GET /prometheus` on `HTTP_SERVER_PORT` (default 8081). With `N` (the default) no
registry exists and nothing is recorded.

| Prometheus series | Meaning |
|---|---|
| `messageserver_messages_processed_total` | messages applied to the graph |
| `messageserver_messages_failed_total` | messages rejected (validation or Neo4j data errors) |
| `messageserver_batch_duration_seconds_{count,sum,max}` | wall time of `MessageBatchProcessor.process` per batch |
| `messageserver_stage_duration_seconds_{count,sum,max}{stage="..."}` | one sample per batch per stage (below) |
| `messageserver_write_transactions_total` | write transactions per batch: 2 for the sample message (nodes, then relationships), at most 3 |
| `messageserver_write_retries_total` | re-runs after a transient error (lock timeout, deadlock, leader change); only present once non-zero |

## Stages (`StageTimings`)
| `stage` | What is timed |
|---|---|
| `parse` | JSON schema validation + JSON -> node/connection objects (includes messages that fail) |
| `group` | computing `signature()` and sorting nodes/connections into statement groups |
| `index` | ensuring indexes exist (a set lookup once an index has been seen) |
| `statement` | the `UNWIND ... MERGE` statement, sent and fully consumed (summed over transient retries) |
| `commit` | the rest of `executeWrite`: begin, commit, retry backoff |
| `ack` | acknowledging the batch's deliveries to RabbitMQ (timed in `MessageServerCommand`, not part of the batch) |

Stages do not overlap, so `parse + group + index + statement + commit` is at most the batch duration. The remainder
(typically 2-4%) is untimed glue: generating Cypher text, session open/close, list building. When a batch is bisected
after a data error, the re-run statements are added to `statement`/`commit` once each.

Average time per stage: `rate(..._sum{stage="x"}[1m]) / rate(..._count{stage="x"}[1m])`. Share of batch time:
`sum(rate(..._sum{stage="x"}[5m])) / sum(rate(messageserver_batch_duration_seconds_sum[5m]))`.

The benchmark (`./gradlew benchmark`) reads the same timings and prints the share per stage for each profile.
