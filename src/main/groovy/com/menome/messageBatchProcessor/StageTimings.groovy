package com.menome.messageBatchProcessor

import groovy.transform.CompileStatic

import java.time.Duration

/**
 * Wall-clock time one batch spent in each stage of MessageBatchProcessor.process (KANBAN E1-2). Not thread safe: a
 * batch is processed on one thread.
 *
 * parse      schema validation + JSON -> node/connection objects
 * group      computing signatures and sorting nodes/connections into statement groups
 * index      ensuring indexes exist (a set lookup once the index has been seen)
 * statement  the UNWIND/MERGE statement itself, sent and fully consumed
 * commit     everything else inside executeWrite: begin, commit and transient-retry backoff
 * ack        acknowledging the deliveries to RabbitMQ (timed by the caller, never part of a batch)
 *
 * Time spent bisecting a failed batch lands in statement/commit of the smaller statements it re-runs.
 */
@CompileStatic
class StageTimings {

    static final String PARSE = "parse"
    static final String GROUP = "group"
    static final String INDEX = "index"
    static final String STATEMENT = "statement"
    static final String COMMIT = "commit"
    static final String ACK = "ack"

    /** The stages MessageBatchProcessor measures, in pipeline order. */
    static final List<String> BATCH_STAGES = [PARSE, GROUP, INDEX, STATEMENT, COMMIT].asImmutable()

    private final Map<String, Long> nanos = new LinkedHashMap<>()

    /**
     * Times executeWrite re-ran a statement after a transient error (lock timeout, deadlock, leader change). The
     * backoff is in `commit`. Zero for a single writer; the first sign of contention once there are several.
     */
    int retries = 0

    /** Write transactions this batch committed or attempted (a retried transaction counts once). 2 for the sample message. */
    int transactions = 0

    StageTimings() {
        for (String stage : BATCH_STAGES) nanos.put(stage, 0L)
    }

    void add(String stage, long elapsedNanos) {
        nanos.put(stage, nanos.getOrDefault(stage, 0L) + elapsedNanos)
    }

    Duration duration(String stage) {
        Duration.ofNanos(nanos.getOrDefault(stage, 0L))
    }

    Duration total() {
        long sum = 0
        for (Long n : nanos.values()) sum += n
        Duration.ofNanos(sum)
    }

    /** Stage -> time, in pipeline order. */
    Map<String, Duration> durations() {
        Map<String, Duration> result = new LinkedHashMap<>()
        for (Map.Entry<String, Long> entry : nanos.entrySet()) result.put(entry.key, Duration.ofNanos(entry.value))
        result
    }

    @Override
    String toString() {
        durations().collect { String stage, Duration d -> "${stage}=${d.toNanos().intdiv(1000L)}us".toString() }.join(" ")
    }
}
