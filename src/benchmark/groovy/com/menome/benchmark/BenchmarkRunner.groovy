package com.menome.benchmark

import com.menome.message.Neo4JNode
import com.menome.messageBatchProcessor.BatchPipeline
import com.menome.messageBatchProcessor.MessageBatchProcessor
import com.menome.messageBatchProcessor.MessageBatchResult
import com.menome.util.ApplicationConfiguration
import com.menome.util.Cypher
import com.menome.util.Neo4J
import com.menome.util.PreferenceType
import org.neo4j.driver.Driver
import org.neo4j.driver.Session

import com.menome.messageBatchProcessor.StageTimings

import reactor.core.publisher.Flux

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import java.util.function.BiConsumer
import java.time.LocalDate

/**
 * Throughput benchmark (KANBAN E1-1): pushes each workload through MessageBatchProcessor.process against the Neo4j
 * configured by the NEO4J_* environment variables and reports msg/s, writes/s and batch time percentiles.
 * RabbitMQ is deliberately out of the loop so the numbers isolate parse + group + write.
 *
 *   ./gradlew benchmark --args="--profiles=node-only,hubs3 --messages=20000 --batch=500 --warmup=2 --out=docs/benchmarks"
 *
 * It deletes everything tagged SourceSystem=menome_benchmark before and after each profile, so point it at a
 * disposable database.
 */
class BenchmarkRunner {

    static class Options {
        List<String> profiles = Workloads.ids()
        int messages = 20_000
        int batch = 500
        int warmup = 2
        long seed = 42
        String label = "local"
        String out = null
        /** Docker container running Neo4j: image, CPU and memory limits are read from Docker and echoed into the report. */
        String container = null
        /** Free-text description of the machine running Neo4j, for when it is not a local container. */
        String serverHardware = null
        /** 0: parse and write one batch after the other on one thread (as before the pipeline existed). 1+: BatchPipeline with that many writers. */
        int writers = 0
        /** Parser threads of the pipeline; ignored when writers is 0. */
        int parsers = 1
    }

    static class Result {
        Workload workload
        int measuredMessages
        double seconds
        double messagesPerSecond
        double writesPerSecond
        double p50Ms
        double p99Ms
        long nodesInGraph
        /** Stage -> share of measured batch time (percent), see StageTimings. */
        Map<String, Double> stagePercent
        /** Percent of batch time the stages account for; the rest is untimed glue (statement generation, session handling). */
        double accountedPercent
        /** executeWrite re-runs after transient errors (lock timeouts, deadlocks) over the measured batches. */
        int retries
        /** Plans of the first batch after the run: primary keys already in the graph (MERGE matches). */
        List<PlanReport.StatementPlan> plansMatch
        /** Plans of a batch of never-seen keys (MERGE creates). */
        List<PlanReport.StatementPlan> plansCreate
    }

    static void main(String[] args) {
        int status
        try {
            status = run(parse(args))
        } catch (IllegalArgumentException | IllegalStateException refusal) {
            System.err.println(refusal.message)
            status = 2
        }
        System.exit(status)
    }

    static Options parse(String[] args) {
        Options options = new Options()
        args.each { String arg ->
            String[] kv = arg.replaceFirst("^--", "").split("=", 2)
            if (kv.length != 2) throw new IllegalArgumentException("Expected --name=value, got '$arg'")
            switch (kv[0]) {
                case "profiles": options.profiles = kv[1].split(",").collect { it.trim() }; break
                case "messages": options.messages = kv[1] as int; break
                case "batch": options.batch = kv[1] as int; break
                case "warmup": options.warmup = kv[1] as int; break
                case "seed": options.seed = kv[1] as long; break
                case "label": options.label = kv[1]; break
                case "out": options.out = kv[1]; break
                case "writers": options.writers = kv[1] as int; break
                case "parsers": options.parsers = kv[1] as int; break
                case "container": options.container = kv[1]; break
                case "server-hardware": options.serverHardware = kv[1]; break
                default: throw new IllegalArgumentException("Unknown option '--${kv[0]}'")
            }
        }
        if (options.messages <= options.warmup * options.batch) {
            throw new IllegalArgumentException("--messages must exceed warmup x batch (${options.warmup * options.batch}) so something is measured")
        }
        options
    }

    /** Returns the process exit status: non-zero if any batch had failed messages or the graph holds the wrong node count. */
    static int run(Options options) {
        Driver driver = Neo4J.openDriver()
        try {
            driver.verifyConnectivity()
            // Throws (and nothing is written) unless the server's environment can be described in the report
            String environment = "# Benchmark ${options.label} - ${LocalDate.now()}\n\n## Environment\n" + EnvironmentInfo.capture(driver, options)
            List<Result> results = []
            boolean healthy = true
            options.profiles.each { String profile ->
                Workload workload = Workloads.build(profile, options.messages, options.seed)
                println "\n== ${profile}: ${workload.description}"
                cleanup(driver)
                try {
                    Result result = measure(workload, options, driver)
                    result.plansMatch = PlanReport.profile(driver, workload, options.batch)
                    result.plansCreate = PlanReport.profile(driver,
                            Workloads.build(profile, options.batch, options.seed + 1, options.messages + 1_000_000L), options.batch)
                    results.add(result)
                    println "   ${format(result)}"
                    if (result.nodesInGraph != workload.messages.size()) {
                        healthy = false
                        println "   !! expected ${workload.messages.size()} ${workload.nodeType} nodes, found ${result.nodesInGraph}"
                    }
                } catch (IllegalStateException failure) {
                    healthy = false
                    println "   !! ${failure.message}"
                } finally {
                    cleanup(driver)
                }
            }
            String report = report(environment, options, results)
            println "\n" + report
            if (options.out) {
                File file = new File(options.out, "${LocalDate.now()}-${options.label}.md")
                file.parentFile.mkdirs()
                file.text = report
                println "Report written to ${file.path}"
            }
            healthy ? 0 : 1
        } finally {
            driver.close()
        }
    }

    private static Result measure(Workload workload, Options options, Driver driver) {
        options.writers == 0 ? measureSerial(workload, options, driver) : measurePipelined(workload, options, driver)
    }

    /** One batch after the other: parse, write, parse, write. Time is the sum of the batches. */
    private static Result measureSerial(Workload workload, Options options, Driver driver) {
        List<List<String>> batches = workload.messages.collate(options.batch)
        List<MessageBatchResult> results = []
        List<Double> batchMillis = []
        batches.eachWithIndex { List<String> batch, int index ->
            long start = System.nanoTime()
            MessageBatchResult result = MessageBatchProcessor.process(batch, driver)
            double millis = (System.nanoTime() - start) / 1_000_000d
            requireNoFailures(result, index)
            if (index >= options.warmup) {
                results.add(result)
                batchMillis.add(millis)
            }
        }
        aggregate(workload, results, batchMillis, batchMillis.sum() / 1000d, driver)
    }

    /**
     * The batches after the warm-up go through BatchPipeline (the same code the server runs). Time is the wall clock of
     * that whole run, because batches overlap: the sum of their own work would be longer than the run.
     */
    private static Result measurePipelined(Workload workload, Options options, Driver driver) {
        List<List<String>> batches = workload.messages.collate(options.batch)
        // Warm-up on one thread: JIT, schema creation, caches. Not measured.
        batches.take(options.warmup).eachWithIndex { List<String> batch, int index -> requireNoFailures(MessageBatchProcessor.process(batch, driver), index) }

        List<BatchPipeline.Batch<Integer>> input = batches.drop(options.warmup).indexed().collect { int index, List<String> batch ->
            new BatchPipeline.Batch<Integer>(index + options.warmup, batch)
        }
        AtomicReference<Throwable> failure = new AtomicReference<>()
        long start = System.nanoTime()
        List<BatchPipeline.Processed<Integer>> processed = BatchPipeline.run(Flux.fromIterable(input), driver, null, options.parsers, options.writers,
                { Integer token, MessageBatchResult result -> requireNoFailures(result, token) } as BiConsumer,
                { Integer token, Throwable error -> failure.compareAndSet(null, error) } as BiConsumer, Duration.ZERO)
                .collectList().block()
        double seconds = (System.nanoTime() - start) / 1e9d
        if (failure.get() != null) throw new IllegalStateException("a batch failed in the pipeline: ${failure.get().message}", failure.get())
        List<MessageBatchResult> results = processed*.result
        // Each batch's own work (parse + write); the batches overlap, so these do not add up to the wall clock
        aggregate(workload, results, results.collect { it.batchSummary.batchProcessingDuration.toNanos() / 1_000_000d }, seconds, driver)
    }

    private static void requireNoFailures(MessageBatchResult result, int index) {
        if (result.batchSummary.errorCount > 0) {
            throw new IllegalStateException("batch ${index} had ${result.batchSummary.errorCount} failed messages: ${result.errors.first().errorText}")
        }
    }

    private static Result aggregate(Workload workload, List<MessageBatchResult> results, List<Double> batchMillis, double seconds, Driver driver) {
        Map<String, Long> stageNanos = StageTimings.BATCH_STAGES.collectEntries { [(it): 0L] }
        long batchNanos = 0
        int measuredMessages = 0
        int retries = 0
        results.each { MessageBatchResult result ->
            measuredMessages += result.batchSummary.successCount + result.batchSummary.errorCount
            batchNanos += result.batchSummary.batchProcessingDuration.toNanos()
            retries += result.batchSummary.stages.retries
            result.batchSummary.stages.durations().each { String stage, Duration d -> stageNanos[stage] += d.toNanos() }
        }
        new Result(workload: workload, measuredMessages: measuredMessages, seconds: seconds,
                messagesPerSecond: measuredMessages / seconds,
                writesPerSecond: measuredMessages * workload.writesPerMessage / seconds,
                p50Ms: Stats.percentile(batchMillis, 50), p99Ms: Stats.percentile(batchMillis, 99),
                nodesInGraph: countPrimaryNodes(driver, workload.nodeType),
                stagePercent: stageNanos.collectEntries { String stage, Long n -> [(stage): n * 100d / batchNanos] },
                accountedPercent: stageNanos.values().sum() * 100d / batchNanos, retries: retries)
    }

    private static long countPrimaryNodes(Driver driver, String nodeType) {
        driver.session().withCloseable { Session session ->
            session.run(Cypher.versioned("MATCH (n:${Cypher.id(nodeType)}) WHERE n.${Cypher.id(Neo4JNode.keys().sourceSystem)} = \$source RETURN count(n) AS c"),
                    [source: Workloads.SOURCE_SYSTEM]).single().get("c").asLong()
        }
    }

    private static void cleanup(Driver driver) {
        driver.session().withCloseable { Session session ->
            session.run(Cypher.versioned("MATCH (n) WHERE n.${Cypher.id(Neo4JNode.keys().sourceSystem)} = \$source CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS"),
                    [source: Workloads.SOURCE_SYSTEM]).consume()
        }
    }

    private static String format(Result r) {
        String.format("%,.0f msg/s  %,.0f writes/s  p50 %.1f ms  p99 %.1f ms  (%,d messages in %.2f s)\n   stages: %s  (accounted %.1f%%), transient retries %d",
                r.messagesPerSecond, r.writesPerSecond, r.p50Ms, r.p99Ms, r.measuredMessages, r.seconds,
                r.stagePercent.collect { stage, pct -> String.format("%s %.1f%%", stage, pct) }.join(", "), r.accountedPercent, r.retries)
    }

    private static String report(String environment, Options options, List<Result> results) {
        StringBuilder sb = new StringBuilder(environment)
        sb << "\n\n## Results\n\n"
        sb << "| Profile | msg/s | writes/s | p50 batch ms | p99 batch ms | measured msgs | transient retries |\n|---|---|---|---|---|---|---|\n"
        results.each { Result r ->
            sb << String.format("| %s | %,.0f | %,.0f | %.1f | %.1f | %,d | %d |\n", r.workload.id, r.messagesPerSecond, r.writesPerSecond, r.p50Ms, r.p99Ms, r.measuredMessages, r.retries)
        }
        sb << "\n## Where batch time goes (% of batch time)\n\n"
        sb << "| Profile | ${StageTimings.BATCH_STAGES.join(' | ')} | accounted |\n|---|${'---|' * (StageTimings.BATCH_STAGES.size() + 1)}\n"
        results.each { Result r ->
            sb << "| ${r.workload.id} | ${StageTimings.BATCH_STAGES.collect { String.format('%.1f', r.stagePercent[it]) }.join(' | ')} | ${String.format('%.1f', r.accountedPercent)} |\n"
        }
        sb << "\nstatement = the UNWIND/MERGE round trip; commit = begin + commit + retry backoff inside executeWrite. `ack` is only timed in the server (RabbitMQ is not in this benchmark).\n"
        sb << "\nWrites = primary node merge + one relationship merge per connection (hub node merges not counted).\n"
        sb << "Transient retries = times executeWrite re-ran a statement after a lock/deadlock-type error; the only contention signal this Neo4j edition exposes (no server metrics over Bolt).\n"
        sb << "\n## Neo4j plans (PROFILE)\n\n"
        sb << "The statements MessageBatchProcessor generates for the first batch of each profile, in execution order, profiled in one rolled-back transaction. "
        sb << "Page cache hit % appears only when PROFILE reports page cache stats (n/a on Community); server-wide page cache ratio and lock waits are not exposed by this edition over Bolt.\n"
        results.each { Result r ->
            sb << "\n### ${r.workload.id}\n\n"
            sb << PlanReport.markdown("Primary keys already in the graph (MERGE matches)", r.plansMatch) << "\n"
            sb << PlanReport.markdown("Never-seen keys (MERGE creates)", r.plansCreate) << "\n"
        }
        sb.toString()
    }
}
