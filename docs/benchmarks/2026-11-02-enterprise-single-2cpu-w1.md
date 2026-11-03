# Benchmark enterprise-single-2cpu-w1 - 2026-11-02

## Environment
- Neo4j: 2026.10.1 enterprise at localhost:7699
- Container `ent-single`: image `neo4j:2026.10.1-enterprise` (id sha256:4f80cb3b46eaa87a5fb2a21969011ae098a589527bf12bd1892b9a5ee6b0e23d, digests ["neo4j@sha256:4f80cb3b46eaa87a5fb2a21969011ae098a589527bf12bd1892b9a5ee6b0e23d"])
- Container limits: CPUs 2.000000000, memory 3 GB
- Docker host VM: 8 CPUs, 17009807360 bytes memory, Docker 29.9.2
- Neo4j settings in effect (not set = server default):
  - `server.memory.heap.initial_size` = 1.00GiB
  - `server.memory.heap.max_size` = 1.00GiB
  - `server.memory.pagecache.size` = 1.00GiB
  - `db.tx_log.rotation.size` = 256.00MiB
  - `db.tx_log.rotation.retention_policy` = 2 days 2G
  - `db.checkpoint` = PERIODIC
  - `db.checkpoint.interval.time` = 15m
  - `db.checkpoint.interval.tx` = 100000
  - `db.checkpoint.iops.limit` = 600
  - `db.query.default_language` = CYPHER_25
  - `dbms.cypher.transactions.default_subquery_batch_strategy` = DEFAULT
- Client: Java 21.0.8, 16 cores, 48 GB RAM, max heap 12288 MB, Mac OS X 26.7.1 aarch64
- Cypher: CYPHER 25
- Run: 150000 messages per profile, batch 500, first 10 batches are warm-up and not measured, seed 42
- Processing: 1 parser and 1 writer thread(s) (BatchPipeline)

## Results

| Profile | msg/s | writes/s | p50 batch ms | p99 batch ms | measured msgs | transient retries |
|---|---|---|---|---|---|---|
| node-only | 35,467 | 35,467 | 14.4 | 47.5 | 145,000 | 0 |
| hubs3 | 17,749 | 70,994 | 33.5 | 55.8 | 145,000 | 0 |
| hub-fanout | 29,193 | 58,385 | 19.7 | 39.9 | 145,000 | 0 |
| wide | 8,251 | 33,004 | 73.5 | 131.6 | 145,000 | 0 |
| unique-dest | 12,439 | 49,755 | 46.5 | 70.9 | 145,000 | 0 |

## Where batch time goes (% of batch time)

| Profile | parse | group | index | statement | commit | accounted |
|---|---|---|---|---|---|---|
| node-only | 14.3 | 3.7 | 0.2 | 48.8 | 30.6 | 97.6 |
| hubs3 | 15.4 | 6.2 | 0.1 | 50.5 | 23.7 | 95.8 |
| hub-fanout | 15.2 | 4.7 | 0.2 | 46.0 | 31.1 | 97.2 |
| wide | 16.0 | 4.8 | 0.1 | 58.6 | 17.6 | 97.0 |
| unique-dest | 11.8 | 5.0 | 0.1 | 57.1 | 23.2 | 97.3 |

statement = the UNWIND/MERGE round trip; commit = begin + commit + retry backoff inside executeWrite. `ack` is only timed in the server (RabbitMQ is not in this benchmark).

Writes = primary node merge + one relationship merge per connection (hub node merges not counted).
Transient retries = times executeWrite re-ran a statement after a lock/deadlock-type error; the only contention signal this Neo4j edition exposes (no server metrics over Bolt).

## Neo4j plans (PROFILE)

The statements MessageBatchProcessor generates for the first batch of each profile, in execution order, profiled in one rolled-back transaction. Page cache hit % appears only when PROFILE reports page cache stats (n/a on Community); server-wide page cache ratio and lock waits are not exposed by this edition over Bolt.

### node-only

**Primary keys already in the graph (MERGE matches)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 5,500 | 11.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`Prop1` = param.`Prop1`, n.`Prop2` = param.`Prop2`, n.`Prop3` = param.`Prop3`, n.`Prop4` = param.`Prop4`, n.`Prop5` = param.`Prop5`, n.`SourceSystem` = param.`SourceSystem` `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=4500  n.Name = param.Name, n.Priority = param.Priority, n.Prop1 = param.Prop1, n.Prop2 = param.Prop2, n.Prop3 = p...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=1526/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

**Never-seen keys (MERGE creates)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 5,500 | 11.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`Prop1` = param.`Prop1`, n.`Prop2` = param.`Prop2`, n.`Prop3` = param.`Prop3`, n.`Prop4` = param.`Prop4`, n.`Prop5` = param.`Prop5`, n.`SourceSystem` = param.`SourceSystem` `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=4500  n.Name = param.Name, n.Priority = param.Priority, n.Prop1 = param.Prop1, n.Prop2 = param.Prop2, n.Prop3 = p...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=3000/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>


### hubs3

**Primary keys already in the graph (MERGE matches)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 5,500 | 11.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`Prop1` = param.`Prop1`, n.`Prop2` = param.`Prop2`, n.`Prop3` = param.`Prop3`, n.`Prop4` = param.`Prop4`, n.`Prop5` = param.`Prop5`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 3 | 15 | 5.0 | 100.0 | none | `` MERGE (n:`BenchOffice` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 3 | 15 | 5.0 | 100.0 | none | `` MERGE (n:`BenchProject` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 3 | 15 | 5.0 | 100.0 | none | `` MERGE (n:`BenchTeam` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchOffice` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`WORKS_IN`]->(d) `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchProject` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`WORKS_ON`]->(d) `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchTeam` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`MEMBER_OF`]->(d) `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=4500  n.Name = param.Name, n.Priority = param.Priority, n.Prop1 = param.Prop1, n.Prop2 = param.Prop2, n.Prop3 = p...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=1527/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>node plan: MERGE (n:`BenchOffice` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID()</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=3 dbHits=9  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=3 dbHits=0
        +Unwind@neo4j  rows=3 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=3 dbHits=6 pageCache=5/0  UNIQUE n:BenchOffice(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLin...
```

</details>

<details><summary>node plan: MERGE (n:`BenchProject` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=3 dbHits=9  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=3 dbHits=0
        +Unwind@neo4j  rows=3 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=3 dbHits=6 pageCache=3/0  UNIQUE n:BenchProject(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLi...
```

</details>

<details><summary>node plan: MERGE (n:`BenchTeam` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=3 dbHits=9  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=3 dbHits=0
        +Unwind@neo4j  rows=3 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=3 dbHits=6 pageCache=3/0  UNIQUE n:BenchTeam(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchOffice` {`BenchId`: pa</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MultiNodeIndexSeek@neo4j  rows=500 dbHits=2000 pageCache=2002/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`, UNIQUE d:BenchOffice(BenchId) WHERE BenchId ...
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:WORKS_IN]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=9/0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchProject` {`BenchId`: p</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MultiNodeIndexSeek@neo4j  rows=500 dbHits=2000 pageCache=2002/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`, UNIQUE d:BenchProject(BenchId) WHERE BenchId...
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:WORKS_ON]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=9/0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchTeam` {`BenchId`: para</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MultiNodeIndexSeek@neo4j  rows=500 dbHits=2000 pageCache=2002/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`, UNIQUE d:BenchTeam(BenchId) WHERE BenchId = ...
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:MEMBER_OF]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=9/0  s, d
```

</details>

**Never-seen keys (MERGE creates)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 5,500 | 11.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`Prop1` = param.`Prop1`, n.`Prop2` = param.`Prop2`, n.`Prop3` = param.`Prop3`, n.`Prop4` = param.`Prop4`, n.`Prop5` = param.`Prop5`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 3 | 15 | 5.0 | 100.0 | none | `` MERGE (n:`BenchOffice` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 3 | 15 | 5.0 | 100.0 | none | `` MERGE (n:`BenchProject` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 3 | 15 | 5.0 | 100.0 | none | `` MERGE (n:`BenchTeam` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchOffice` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`WORKS_IN`]->(d) `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchProject` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`WORKS_ON`]->(d) `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchTeam` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`MEMBER_OF`]->(d) `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=4500  n.Name = param.Name, n.Priority = param.Priority, n.Prop1 = param.Prop1, n.Prop2 = param.Prop2, n.Prop3 = p...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=3000/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>node plan: MERGE (n:`BenchOffice` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID()</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=3 dbHits=9  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=3 dbHits=0
        +Unwind@neo4j  rows=3 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=3 dbHits=6 pageCache=5/0  UNIQUE n:BenchOffice(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLin...
```

</details>

<details><summary>node plan: MERGE (n:`BenchProject` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=3 dbHits=9  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=3 dbHits=0
        +Unwind@neo4j  rows=3 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=3 dbHits=6 pageCache=3/0  UNIQUE n:BenchProject(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLi...
```

</details>

<details><summary>node plan: MERGE (n:`BenchTeam` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=3 dbHits=9  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=3 dbHits=0
        +Unwind@neo4j  rows=3 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=3 dbHits=6 pageCache=3/0  UNIQUE n:BenchTeam(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchOffice` {`BenchId`: pa</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MultiNodeIndexSeek@neo4j  rows=500 dbHits=2000 pageCache=1000/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`, UNIQUE d:BenchOffice(BenchId) WHERE BenchId ...
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:WORKS_IN]->(d)
        +Argument@neo4j  rows=500 dbHits=0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchProject` {`BenchId`: p</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MultiNodeIndexSeek@neo4j  rows=500 dbHits=2000 pageCache=1000/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`, UNIQUE d:BenchProject(BenchId) WHERE BenchId...
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:WORKS_ON]->(d)
        +Argument@neo4j  rows=500 dbHits=0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchTeam` {`BenchId`: para</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MultiNodeIndexSeek@neo4j  rows=500 dbHits=2000 pageCache=1000/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`, UNIQUE d:BenchTeam(BenchId) WHERE BenchId = ...
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:MEMBER_OF]->(d)
        +Argument@neo4j  rows=500 dbHits=0  s, d
```

</details>


### hub-fanout

**Primary keys already in the graph (MERGE matches)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 5,500 | 11.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`Prop1` = param.`Prop1`, n.`Prop2` = param.`Prop2`, n.`Prop3` = param.`Prop3`, n.`Prop4` = param.`Prop4`, n.`Prop5` = param.`Prop5`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 1 | 5 | 5.0 | 100.0 | none | `` MERGE (n:`BenchHub` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchHub` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`POINTS_AT`]->(d) `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=4500  n.Name = param.Name, n.Priority = param.Priority, n.Prop1 = param.Prop1, n.Prop2 = param.Prop2, n.Prop3 = p...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=1530/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>node plan: MERGE (n:`BenchHub` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=1 dbHits=3  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=1 dbHits=0
        +Unwind@neo4j  rows=1 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=1 dbHits=2 pageCache=3/0  UNIQUE n:BenchHub(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkAd...
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchHub` {`BenchId`: param</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE d:BenchHub(BenchId) WHERE BenchId = param.`d:BenchId`
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:POINTS_AT]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=9/0  s, d
```

</details>

**Never-seen keys (MERGE creates)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 5,500 | 11.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`Prop1` = param.`Prop1`, n.`Prop2` = param.`Prop2`, n.`Prop3` = param.`Prop3`, n.`Prop4` = param.`Prop4`, n.`Prop5` = param.`Prop5`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 1 | 5 | 5.0 | 100.0 | none | `` MERGE (n:`BenchHub` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchHub` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`POINTS_AT`]->(d) `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=4500  n.Name = param.Name, n.Priority = param.Priority, n.Prop1 = param.Prop1, n.Prop2 = param.Prop2, n.Prop3 = p...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=3000/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>node plan: MERGE (n:`BenchHub` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=1 dbHits=3  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=1 dbHits=0
        +Unwind@neo4j  rows=1 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=1 dbHits=2 pageCache=3/0  UNIQUE n:BenchHub(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkAd...
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchHub` {`BenchId`: param</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE d:BenchHub(BenchId) WHERE BenchId = param.`d:BenchId`
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:POINTS_AT]->(d)
        +Argument@neo4j  rows=500 dbHits=0  s, d
```

</details>


### wide

**Primary keys already in the graph (MERGE matches)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 23,500 | 47.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`DATE_0` = param.`DATE_0`, n.`DATE_1` = param.`DATE_1`, n.`DATE_2` = param.`DATE_2`, n.`DATE_3` = param.`DATE_3`, n.`DATE_4` = param.`DATE_4`, n.`DATE_5` = param.`DATE_5`, n.`DATE_6` = param.`DATE_6`, n.`DATE_7` = param.`DATE_7`, n.`DATE_8` = param.`DATE_8`, n.`DATE_9` = param.`DATE_9`, n.`NUM_0` = param.`NUM_0`, n.`NUM_1` = param.`NUM_1`, n.`NUM_2` = param.`NUM_2`, n.`NUM_3` = param.`NUM_3`, n.`NUM_4` = param.`NUM_4`, n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`SourceSystem` = param.`SourceSystem`, n.`TEXT_0` = param.`TEXT_0`, n.`TEXT_1` = param.`TEXT_1`, n.`TEXT_10` = param.`TEXT_10`, n.`TEXT_11` = param.`TEXT_11`, n.`TEXT_12` = param.`TEXT_12`, n.`TEXT_13` = param.`TEXT_13`, n.`TEXT_14` = param.`TEXT_14`, n.`TEXT_15` = param.`TEXT_15`, n.`TEXT_16` = param.`TEXT_16`, n.`TEXT_17` = param.`TEXT_17`, n.`TEXT_18` = param.`TEXT_18`, n.`TEXT_19` = param.`TEXT_19`, n.`TEXT_2` = param.`TEXT_2`, n.`TEXT_20` = param.`TEXT_20`, n.`TEXT_21` = param.`TEXT_21`, n.`TEXT_22` = param.`TEXT_22`, n.`TEXT_23` = param.`TEXT_23`, n.`TEXT_24` = param.`TEXT_24`, n.`TEXT_25` = param.`TEXT_25`, n.`TEXT_3` = param.`TEXT_3`, n.`TEXT_4` = param.`TEXT_4`, n.`TEXT_5` = param.`TEXT_5`, n.`TEXT_6` = param.`TEXT_6`, n.`TEXT_7` = param.`TEXT_7`, n.`TEXT_8` = param.`TEXT_8`, n.`TEXT_9` = param.`TEXT_9` `` |
| node | 478 | 2,390 | 5.0 | 100.0 | none | `` MERGE (n:`BenchAccount` {`ACCTNUM`: param.`ACCTNUM`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 10 | 50 | 5.0 | 100.0 | none | `` MERGE (n:`BenchActivity` {`ACTIVITY_CODE`: param.`ACTIVITY_CODE`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 474 | 2,370 | 5.0 | 100.0 | none | `` MERGE (n:`BenchDialer` {`ACCTNUM`: param.`ACCTNUM`, `SNAPSHOT`: param.`SNAPSHOT`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchAccount` {`ACCTNUM`: param.`d:ACCTNUM`}) MERGE (s)-[r:`ENTRY_FOR_ACCOUNT`]->(d) `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchActivity` {`ACTIVITY_CODE`: param.`d:ACTIVITY_CODE`}) MERGE (s)-[r:`ACTIVITY`]->(d) `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchDialer` {`ACCTNUM`: param.`d:ACCTNUM`, `SNAPSHOT`: param.`d:SNAPSHOT`}) MERGE (s)-[r:`DIALER`]->(d) `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=22500  n.DATE_0 = param.DATE_0, n.DATE_1 = param.DATE_1, n.DATE_2 = param.DATE_2, n.DATE_3 = param.DATE_3, n.DATE_...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=1611/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>node plan: MERGE (n:`BenchAccount` {`ACCTNUM`: param.`ACCTNUM`}) ON CREATE SET n.`Uuid` = randomUUID(</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=478 dbHits=1434  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=478 dbHits=0
        +Unwind@neo4j  rows=478 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=478 dbHits=956 pageCache=1816/0  UNIQUE n:BenchAccount(ACCTNUM) WHERE ACCTNUM = param.ACCTNUM,  ON CREATE SET n.Uuid = randomUUID(), n.TheLi...
```

</details>

<details><summary>node plan: MERGE (n:`BenchActivity` {`ACTIVITY_CODE`: param.`ACTIVITY_CODE`}) ON CREATE SET n.`Uuid` </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=10 dbHits=30  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=10 dbHits=0
        +Unwind@neo4j  rows=10 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=10 dbHits=20 pageCache=12/0  UNIQUE n:BenchActivity(ACTIVITY_CODE) WHERE ACTIVITY_CODE = param.ACTIVITY_CODE,  ON CREATE SET n.Uuid = ra...
```

</details>

<details><summary>node plan: MERGE (n:`BenchDialer` {`ACCTNUM`: param.`ACCTNUM`, `SNAPSHOT`: param.`SNAPSHOT`}) ON CREA</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=474 dbHits=1422  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=474 dbHits=0
        +Unwind@neo4j  rows=474 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=474 dbHits=948 pageCache=1785/0  UNIQUE n:BenchDialer(ACCTNUM, SNAPSHOT) WHERE ACCTNUM = param.ACCTNUM AND SNAPSHOT = param.SNAPSHOT,  ON CR...
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchAccount` {`ACCTNUM`: p</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1006/0  UNIQUE d:BenchAccount(ACCTNUM) WHERE ACCTNUM = param.`d:ACCTNUM`
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:ENTRY_FOR_ACCOUNT]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=8/0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchActivity` {`ACTIVITY_C</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE d:BenchActivity(ACTIVITY_CODE) WHERE ACTIVITY_CODE = param.`d:ACTIVITY_CODE`
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:ACTIVITY]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=8/0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchDialer` {`ACCTNUM`: pa</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1010/0  UNIQUE d:BenchDialer(ACCTNUM, SNAPSHOT) WHERE ACCTNUM = param.`d:ACCTNUM` AND SNAPSHOT = param.`d:SNAPSHOT`
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:DIALER]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=8/0  s, d
```

</details>

**Never-seen keys (MERGE creates)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 23,500 | 47.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`DATE_0` = param.`DATE_0`, n.`DATE_1` = param.`DATE_1`, n.`DATE_2` = param.`DATE_2`, n.`DATE_3` = param.`DATE_3`, n.`DATE_4` = param.`DATE_4`, n.`DATE_5` = param.`DATE_5`, n.`DATE_6` = param.`DATE_6`, n.`DATE_7` = param.`DATE_7`, n.`DATE_8` = param.`DATE_8`, n.`DATE_9` = param.`DATE_9`, n.`NUM_0` = param.`NUM_0`, n.`NUM_1` = param.`NUM_1`, n.`NUM_2` = param.`NUM_2`, n.`NUM_3` = param.`NUM_3`, n.`NUM_4` = param.`NUM_4`, n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`SourceSystem` = param.`SourceSystem`, n.`TEXT_0` = param.`TEXT_0`, n.`TEXT_1` = param.`TEXT_1`, n.`TEXT_10` = param.`TEXT_10`, n.`TEXT_11` = param.`TEXT_11`, n.`TEXT_12` = param.`TEXT_12`, n.`TEXT_13` = param.`TEXT_13`, n.`TEXT_14` = param.`TEXT_14`, n.`TEXT_15` = param.`TEXT_15`, n.`TEXT_16` = param.`TEXT_16`, n.`TEXT_17` = param.`TEXT_17`, n.`TEXT_18` = param.`TEXT_18`, n.`TEXT_19` = param.`TEXT_19`, n.`TEXT_2` = param.`TEXT_2`, n.`TEXT_20` = param.`TEXT_20`, n.`TEXT_21` = param.`TEXT_21`, n.`TEXT_22` = param.`TEXT_22`, n.`TEXT_23` = param.`TEXT_23`, n.`TEXT_24` = param.`TEXT_24`, n.`TEXT_25` = param.`TEXT_25`, n.`TEXT_3` = param.`TEXT_3`, n.`TEXT_4` = param.`TEXT_4`, n.`TEXT_5` = param.`TEXT_5`, n.`TEXT_6` = param.`TEXT_6`, n.`TEXT_7` = param.`TEXT_7`, n.`TEXT_8` = param.`TEXT_8`, n.`TEXT_9` = param.`TEXT_9` `` |
| node | 473 | 2,365 | 5.0 | 100.0 | none | `` MERGE (n:`BenchAccount` {`ACCTNUM`: param.`ACCTNUM`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 10 | 50 | 5.0 | 100.0 | none | `` MERGE (n:`BenchActivity` {`ACTIVITY_CODE`: param.`ACTIVITY_CODE`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 481 | 2,405 | 5.0 | 100.0 | none | `` MERGE (n:`BenchDialer` {`ACCTNUM`: param.`ACCTNUM`, `SNAPSHOT`: param.`SNAPSHOT`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchAccount` {`ACCTNUM`: param.`d:ACCTNUM`}) MERGE (s)-[r:`ENTRY_FOR_ACCOUNT`]->(d) `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchActivity` {`ACTIVITY_CODE`: param.`d:ACTIVITY_CODE`}) MERGE (s)-[r:`ACTIVITY`]->(d) `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchDialer` {`ACCTNUM`: param.`d:ACCTNUM`, `SNAPSHOT`: param.`d:SNAPSHOT`}) MERGE (s)-[r:`DIALER`]->(d) `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=22500  n.DATE_0 = param.DATE_0, n.DATE_1 = param.DATE_1, n.DATE_2 = param.DATE_2, n.DATE_3 = param.DATE_3, n.DATE_...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=3000/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>node plan: MERGE (n:`BenchAccount` {`ACCTNUM`: param.`ACCTNUM`}) ON CREATE SET n.`Uuid` = randomUUID(</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=473 dbHits=1419  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=473 dbHits=0
        +Unwind@neo4j  rows=473 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=473 dbHits=946 pageCache=1870/0  UNIQUE n:BenchAccount(ACCTNUM) WHERE ACCTNUM = param.ACCTNUM,  ON CREATE SET n.Uuid = randomUUID(), n.TheLi...
```

</details>

<details><summary>node plan: MERGE (n:`BenchActivity` {`ACTIVITY_CODE`: param.`ACTIVITY_CODE`}) ON CREATE SET n.`Uuid` </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=10 dbHits=30  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=10 dbHits=0
        +Unwind@neo4j  rows=10 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=10 dbHits=20 pageCache=12/0  UNIQUE n:BenchActivity(ACTIVITY_CODE) WHERE ACTIVITY_CODE = param.ACTIVITY_CODE,  ON CREATE SET n.Uuid = ra...
```

</details>

<details><summary>node plan: MERGE (n:`BenchDialer` {`ACCTNUM`: param.`ACCTNUM`, `SNAPSHOT`: param.`SNAPSHOT`}) ON CREA</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=481 dbHits=1443  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=481 dbHits=0
        +Unwind@neo4j  rows=481 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=481 dbHits=962 pageCache=1917/0  UNIQUE n:BenchDialer(ACCTNUM, SNAPSHOT) WHERE ACCTNUM = param.ACCTNUM AND SNAPSHOT = param.SNAPSHOT,  ON CR...
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchAccount` {`ACCTNUM`: p</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1002/0  UNIQUE d:BenchAccount(ACCTNUM) WHERE ACCTNUM = param.`d:ACCTNUM`
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:ENTRY_FOR_ACCOUNT]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=487/0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchActivity` {`ACTIVITY_C</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE d:BenchActivity(ACTIVITY_CODE) WHERE ACTIVITY_CODE = param.`d:ACTIVITY_CODE`
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:ACTIVITY]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=1/0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchDialer` {`ACCTNUM`: pa</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1003/0  UNIQUE d:BenchDialer(ACCTNUM, SNAPSHOT) WHERE ACCTNUM = param.`d:ACCTNUM` AND SNAPSHOT = param.`d:SNAPSHOT`
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:DIALER]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=492/0  s, d
```

</details>


### unique-dest

**Primary keys already in the graph (MERGE matches)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 5,500 | 11.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`Prop1` = param.`Prop1`, n.`Prop2` = param.`Prop2`, n.`Prop3` = param.`Prop3`, n.`Prop4` = param.`Prop4`, n.`Prop5` = param.`Prop5`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 500 | 2,500 | 5.0 | 100.0 | none | `` MERGE (n:`BenchOffice` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 500 | 2,500 | 5.0 | 100.0 | none | `` MERGE (n:`BenchProject` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 500 | 2,500 | 5.0 | 100.0 | none | `` MERGE (n:`BenchTeam` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchOffice` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`WORKS_IN`]->(d) `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchProject` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`WORKS_ON`]->(d) `` |
| relationship | 500 | 3,000 | 6.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchTeam` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`MEMBER_OF`]->(d) `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=4500  n.Name = param.Name, n.Priority = param.Priority, n.Prop1 = param.Prop1, n.Prop2 = param.Prop2, n.Prop3 = p...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=1917/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>node plan: MERGE (n:`BenchOffice` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID()</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=1500  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=1824/0  UNIQUE n:BenchOffice(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLin...
```

</details>

<details><summary>node plan: MERGE (n:`BenchProject` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=1500  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=1856/0  UNIQUE n:BenchProject(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLi...
```

</details>

<details><summary>node plan: MERGE (n:`BenchTeam` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=1500  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=1748/0  UNIQUE n:BenchTeam(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchOffice` {`BenchId`: pa</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE d:BenchOffice(BenchId) WHERE BenchId = param.`d:BenchId`
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:WORKS_IN]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=8/0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchProject` {`BenchId`: p</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE d:BenchProject(BenchId) WHERE BenchId = param.`d:BenchId`
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:WORKS_ON]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=8/0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchTeam` {`BenchId`: para</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=1502/0  UNIQUE d:BenchTeam(BenchId) WHERE BenchId = param.`d:BenchId`
      +MergeInto@neo4j  rows=500 dbHits=1000  MERGE (s)-[r:MEMBER_OF]->(d)
        +Argument@neo4j  rows=500 dbHits=0 pageCache=8/0  s, d
```

</details>

**Never-seen keys (MERGE creates)**

| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |
|---|---|---|---|---|---|---|
| node | 500 | 5,500 | 11.0 | 100.0 | none | `` MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`Priority` = param.`Priority`, n.`Prop1` = param.`Prop1`, n.`Prop2` = param.`Prop2`, n.`Prop3` = param.`Prop3`, n.`Prop4` = param.`Prop4`, n.`Prop5` = param.`Prop5`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 500 | 2,500 | 5.0 | 100.0 | none | `` MERGE (n:`BenchOffice` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 500 | 2,500 | 5.0 | 100.0 | none | `` MERGE (n:`BenchProject` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| node | 500 | 2,500 | 5.0 | 100.0 | none | `` MERGE (n:`BenchTeam` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime() SET n.`Name` = param.`Name`, n.`SourceSystem` = param.`SourceSystem` `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchOffice` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`WORKS_IN`]->(d) `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchProject` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`WORKS_ON`]->(d) `` |
| relationship | 500 | 2,500 | 5.0 | 100.0 | none | `` MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchTeam` {`BenchId`: param.`d:BenchId`}) MERGE (s)-[r:`MEMBER_OF`]->(d) `` |

<details><summary>node plan: MERGE (n:`BenchNode` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=4500  n.Name = param.Name, n.Priority = param.Priority, n.Prop1 = param.Prop1, n.Prop2 = param.Prop2, n.Prop3 = p...
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=3000/0  UNIQUE n:BenchNode(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>node plan: MERGE (n:`BenchOffice` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID()</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=1500  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=3000/0  UNIQUE n:BenchOffice(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLin...
```

</details>

<details><summary>node plan: MERGE (n:`BenchProject` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=1500  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=3000/0  UNIQUE n:BenchProject(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLi...
```

</details>

<details><summary>node plan: MERGE (n:`BenchTeam` {`BenchId`: param.`BenchId`}) ON CREATE SET n.`Uuid` = randomUUID(), </summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +SetProperties@neo4j  rows=500 dbHits=1500  n.Name = param.Name, n.SourceSystem = param.SourceSystem
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +MergeUniqueNode@neo4j  rows=500 dbHits=1000 pageCache=3000/0  UNIQUE n:BenchTeam(BenchId) WHERE BenchId = param.BenchId,  ON CREATE SET n.Uuid = randomUUID(), n.TheLinkA...
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchOffice` {`BenchId`: pa</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE d:BenchOffice(BenchId) WHERE BenchId = param.`d:BenchId`
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:WORKS_IN]->(d)
        +Argument@neo4j  rows=500 dbHits=0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchProject` {`BenchId`: p</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE d:BenchProject(BenchId) WHERE BenchId = param.`d:BenchId`
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:WORKS_ON]->(d)
        +Argument@neo4j  rows=500 dbHits=0  s, d
```

</details>

<details><summary>relationship plan: MATCH (s:`BenchNode` {`BenchId`: param.`s:BenchId`}) MATCH (d:`BenchTeam` {`BenchId`: para</summary>

```
+ProduceResults@neo4j  rows=0 dbHits=0
  +EmptyResult@neo4j  rows=0 dbHits=0
    +Apply@neo4j  rows=500 dbHits=0
      +Apply@neo4j  rows=500 dbHits=0
        +Unwind@neo4j  rows=500 dbHits=0  $parms AS param
        +CartesianProduct@neo4j  rows=500 dbHits=0
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE s:BenchNode(BenchId) WHERE BenchId = param.`s:BenchId`
          +NodeUniqueIndexSeek(Locking)@neo4j  rows=500 dbHits=1000 pageCache=500/0  UNIQUE d:BenchTeam(BenchId) WHERE BenchId = param.`d:BenchId`
      +MergeInto@neo4j  rows=500 dbHits=500  MERGE (s)-[r:MEMBER_OF]->(d)
        +Argument@neo4j  rows=500 dbHits=0  s, d
```

</details>

