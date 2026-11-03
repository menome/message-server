package com.menome.benchmark

import com.menome.message.Neo4JConnection
import com.menome.message.Neo4JNode
import com.menome.messageBatchProcessor.MessageBatchCollection
import com.menome.messageBatchProcessor.MessageBatchProcessor
import com.menome.messageBatchProcessor.MessageConnectionCollection
import com.menome.messageBatchProcessor.SchemaManager
import com.menome.util.Cypher
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionCallback
import org.neo4j.driver.TransactionContext

import com.menome.messageBatchProcessor.BatchPipeline
import com.menome.messageBatchProcessor.MessageBatchResult
import reactor.core.publisher.Flux

import java.time.Duration
import java.util.function.BiConsumer
import java.security.MessageDigest
import java.time.LocalDate
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * The ceiling (KANBAN E10-1): the most this database, on this hardware, takes for the graph the message server writes.
 *
 * It replays exactly the statements and rows the server would send (the same Cypher, the same rows, in the same lock
 * order), but everything the server does around them is done beforehand and is not timed: parsing, validation,
 * grouping, ordering and building the parameter maps. What is timed is only N raw driver sessions each running
 * `executeWrite` with the batch's two phase transactions (node merges, then relationship merges), pulling prepared
 * batches from a shared list. No pipeline, no stage timing, no failure handling, no RabbitMQ.
 *
 * So `server msg/s / ceiling msg/s` is the share of the database's capacity the server actually uses, and the app-side
 * costs measured while preparing (microseconds per message to parse+group, order and build parameters) explain the rest.
 *
 *   ./gradlew ceiling --args="--container=neo4j-bench --messages=150000 --sessions=1,4,8 --batches=500,2000 --out=docs/benchmarks"
 *
 * Deletes everything tagged SourceSystem=menome_benchmark before and after each run: use a disposable database.
 * The graph it writes is checked against the server's: first on a sample by content fingerprint, then on every run by
 * node and relationship counts.
 */
class CeilingRunner {

    static class Options extends BenchmarkRunner.Options {
        List<Integer> sessions = [1, 4, 8]
        List<Integer> batches = [500]
        /** Messages used for the graph-equality check against the server (not timed). */
        int verifyMessages = 2_000
        /** A new driver session for every batch (what the server does) instead of one session per thread. */
        boolean sessionPerBatch = false
        /** Session / writer counts at which the server's pipeline is measured against the ceiling, interleaved (empty: not at all). */
        List<Integer> compare = []
        /** Repeats of each interleaved pair; the report gives the median and the range. */
        int repeats = 3
    }

    /** One transaction's worth of work: statements in order, each already holding its parameter map. */
    static class Phase {
        List<List> statements = []   // [cypher, parameters]
    }

    static class Prepared {
        List<Phase> phases = []
    }

    static class Prep {
        List<Prepared> batches = []
        double parseGroupMicrosPerMessage
        double orderMicrosPerMessage
        double parametersMicrosPerMessage
        int statementsPerBatch
    }

    static class Run {
        String profile
        int batch
        int sessions
        int measuredMessages
        double seconds
        double messagesPerSecond
        int retries
        String kind = "ceiling"
        long nodes
        long relationships
        long expectedRelationships
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
        options.batch = 500
        args.each { String arg ->
            String[] kv = arg.replaceFirst("^--", "").split("=", 2)
            if (kv.length != 2) throw new IllegalArgumentException("Expected --name=value, got '$arg'")
            switch (kv[0]) {
                case "profiles": options.profiles = kv[1].split(",").collect { it.trim() }; break
                case "messages": options.messages = kv[1] as int; break
                case "batches": options.batches = kv[1].split(",").collect { it.trim() as int }; break
                case "sessions": options.sessions = kv[1].split(",").collect { it.trim() as int }; break
                case "warmup": options.warmup = kv[1] as int; break
                case "seed": options.seed = kv[1] as long; break
                case "label": options.label = kv[1]; break
                case "out": options.out = kv[1]; break
                case "verify-messages": options.verifyMessages = kv[1] as int; break
                case "compare": options.compare = kv[1].split(",").collect { it.trim() as int }; break
                case "repeats": options.repeats = kv[1] as int; break
                case "session-per-batch": options.sessionPerBatch = kv[1].toBoolean(); break
                case "container": options.container = kv[1]; break
                case "server-hardware": options.serverHardware = kv[1]; break
                default: throw new IllegalArgumentException("Unknown option '--${kv[0]}'")
            }
        }
        options.batch = options.batches.first()
        if (options.messages <= options.warmup * options.batches.max()) {
            throw new IllegalArgumentException("--messages must exceed warmup x the largest batch (${options.warmup * options.batches.max()}) so something is measured")
        }
        options
    }

    static int run(Options options) {
        EnvironmentInfo.checkDeclared(options)
        Driver driver = Neo4J.openDriver()
        try {
            driver.verifyConnectivity()
            // The shared environment text also describes the server benchmark's run and pipeline, which do not apply here
            String environment = "# Ceiling ${options.label} - ${LocalDate.now()}\n\n## Environment\n" +
                    EnvironmentInfo.capture(driver, options).readLines().findAll { !it.startsWith("- Run:") && !it.startsWith("- Processing:") }.join("\n")
            boolean healthy = true
            List<Run> runs = []
            Map<String, Prep> preps = [:]
            Map<String, Boolean> equal = [:]
            List<Run> comparison = []
            options.profiles.each { String profile ->
                Workload workload = Workloads.build(profile, options.messages, options.seed)
                println "\n== ${profile}: ${workload.description}"
                equal[profile] = verifyAgainstServer(profile, options, driver)
                println "   graph equal to the server's on ${options.verifyMessages} messages: ${equal[profile]}"
                if (!equal[profile]) healthy = false
                boolean prewarmed = false
                if (options.compare) {
                    int batch = options.batches.first()
                    Prep prep = prepare(workload, batch)
                    options.repeats.times { int repeat ->
                        options.compare.each { int n ->
                            cleanup(driver)
                            try {
                                Run c = measure(workload, prep, options, batch, n, driver)
                                comparison << c
                                println String.format("   [compare %d/%d] ceiling %d session(s): %,.0f msg/s", repeat + 1, options.repeats, n, c.messagesPerSecond)
                            } finally { cleanup(driver) }
                            try {
                                Run v = measureServer(workload, options, batch, n, driver)
                                comparison << v
                                println String.format("   [compare %d/%d] server  %d writer(s):  %,.0f msg/s", repeat + 1, options.repeats, n, v.messagesPerSecond)
                            } finally { cleanup(driver) }
                        }
                    }
                }
                options.batches.each { int batch ->
                    Prep prep = prepare(workload, batch)
                    preps["${profile}/${batch}".toString()] = prep
                    if (!prewarmed) {
                        // The first run on a fresh store is slower (store files and page cache still growing). Run the whole
                        // workload once at the highest session count, unmeasured, so no configuration is penalised for coming first.
                        cleanup(driver)
                        try {
                            measure(workload, prep, options, batch, options.sessions.max(), driver)
                        } finally {
                            cleanup(driver)
                        }
                        prewarmed = true
                        println "   (unmeasured warm-up pass done)"
                    }
                    println String.format("   prepared batch %d: %d statements/batch; per message: parse+group %.1f us, order %.1f us, parameters %.1f us",
                            batch, prep.statementsPerBatch, prep.parseGroupMicrosPerMessage, prep.orderMicrosPerMessage, prep.parametersMicrosPerMessage)
                    options.sessions.each { int sessions ->
                        cleanup(driver)
                        try {
                            Run result = measure(workload, prep, options, batch, sessions, driver)
                            runs << result
                            boolean ok = result.nodes == workload.messages.size() && result.relationships == result.expectedRelationships
                            println String.format("   batch %5d, %d session(s): %,.0f msg/s  (%,d messages in %.2f s, %d retries)%s", batch, sessions,
                                    result.messagesPerSecond, result.measuredMessages, result.seconds, result.retries,
                                    ok ? "" : "   !! nodes ${result.nodes} (want ${workload.messages.size()}), relationships ${result.relationships} (want ${result.expectedRelationships})")
                            if (!ok) healthy = false
                        } finally {
                            cleanup(driver)
                        }
                    }
                }
            }
            String report = report(environment, options, runs, preps, equal, comparison)
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

    /**
     * Everything the server does before it talks to the database, done once and timed per stage so the report can say what
     * the application costs per message. The statements and the order are the server's own (it is its own code).
     */
    static Prep prepare(Workload workload, int batchSize) {
        Prep prep = new Prep()
        long parseNanos = 0, orderNanos = 0, parameterNanos = 0
        long messages = 0
        List<List<String>> chunks = workload.messages.collate(batchSize)
        chunks.each { List<String> chunk ->
            long t0 = System.nanoTime()
            MessageBatchProcessor.ParsedBatch parsed = MessageBatchProcessor.parse(chunk)
            parseNanos += System.nanoTime() - t0
            if (!parsed.errors.isEmpty()) throw new IllegalStateException("ceiling: message failed to parse: ${parsed.errors.first().errorText}")

            long t1 = System.nanoTime()
            List<MessageBatchCollection> nodeGroups = new ArrayList<>(parsed.nodeGroups.values())
            nodeGroups.sort(MessageBatchProcessor.NODE_GROUP_ORDER)
            List<List> nodeSides = nodeGroups.collect { MessageBatchCollection group ->
                List<Neo4JNode> rows = group.nodeList()
                rows.sort(MessageBatchProcessor.NODE_ROW_ORDER)
                [group.prototypeNode().toCypher(), rows]
            }
            List<MessageConnectionCollection> connectionGroups = new ArrayList<>(parsed.connectionGroups.values())
            connectionGroups.sort(MessageBatchProcessor.CONNECTION_GROUP_ORDER)
            List<List> relationshipSides = connectionGroups.collect { MessageConnectionCollection group ->
                List<Neo4JConnection> rows = group.connectionList()
                rows.sort(MessageBatchProcessor.CONNECTION_ROW_ORDER)
                [group.prototypeRelationship().toCypher(), rows]
            }
            orderNanos += System.nanoTime() - t1

            long t2 = System.nanoTime()
            Prepared prepared = new Prepared()
            Phase nodePhase = new Phase()
            nodeSides.each { List side ->
                nodePhase.statements << [Cypher.versioned("UNWIND \$parms AS param " + side[0]), MessageBatchCollection.asParameterList((List<Neo4JNode>) side[1])]
            }
            Phase relationshipPhase = new Phase()
            relationshipSides.each { List side ->
                relationshipPhase.statements << [Cypher.versioned("UNWIND \$parms AS param " + side[0]), MessageConnectionCollection.asParameterList((List<Neo4JConnection>) side[1])]
            }
            parameterNanos += System.nanoTime() - t2
            prepared.phases << nodePhase
            if (!relationshipPhase.statements.isEmpty()) prepared.phases << relationshipPhase
            prep.batches << prepared
            prep.statementsPerBatch = Math.max(prep.statementsPerBatch, nodePhase.statements.size() + relationshipPhase.statements.size())
            messages += chunk.size()
        }
        prep.parseGroupMicrosPerMessage = parseNanos / 1000d / messages
        prep.orderMicrosPerMessage = orderNanos / 1000d / messages
        prep.parametersMicrosPerMessage = parameterNanos / 1000d / messages
        prep
    }

    /** `sessions` raw driver sessions pull prepared batches in order; wall clock covers only the database work. */
    private static Run measure(Workload workload, Prep prep, Options options, int batchSize, int sessions, Driver driver) {
        // Warm-up on one session: constraints created, caches and connections warm. Not measured.
        driver.session().withCloseable { Session session ->
            for (Neo4JNode node : firstNodes(workload)) { SchemaManager.ensure(node, session) }
            prep.batches.take(options.warmup).each { Prepared batch -> write(session, batch, new AtomicInteger()) }
        }
        List<Prepared> measured = prep.batches.drop(options.warmup)
        int measuredMessages = Math.min(workload.messages.size() - options.warmup * batchSize, measured.size() * batchSize)
        AtomicInteger next = new AtomicInteger()
        AtomicInteger retries = new AtomicInteger()
        AtomicLong failure = new AtomicLong()
        List<Throwable> errors = Collections.synchronizedList([])
        long start = System.nanoTime()
        List<Thread> threads = (0..<sessions).collect { int n ->
            Thread.start("ceiling-${n}") {
                Session session = options.sessionPerBatch ? null : driver.session()
                try {
                    int index
                    while ((index = next.getAndIncrement()) < measured.size() && errors.isEmpty()) {
                        if (options.sessionPerBatch) {
                            driver.session().withCloseable { Session each -> write(each, measured[index], retries) }
                        } else {
                            write(session, measured[index], retries)
                        }
                    }
                } catch (Throwable t) {
                    errors << t
                } finally {
                    session?.close()
                }
            }
        }
        threads*.join()
        double seconds = (System.nanoTime() - start) / 1e9d
        if (!errors.isEmpty()) throw new IllegalStateException("a ceiling session failed: ${errors.first()}", errors.first())
        new Run(profile: workload.id, batch: batchSize, sessions: sessions, measuredMessages: measuredMessages, seconds: seconds,
                messagesPerSecond: measuredMessages / seconds, retries: retries.get(),
                nodes: countNodes(driver, workload), relationships: countRelationships(driver, workload),
                expectedRelationships: (workload.writesPerMessage - 1L) * workload.messages.size())
    }

    /** The real server code path (BatchPipeline, one parser) over the same workload, timed the way BenchmarkRunner does. */
    private static Run measureServer(Workload workload, Options options, int batchSize, int writers, Driver driver) {
        List<List<String>> batches = workload.messages.collate(batchSize)
        batches.take(options.warmup).each { List<String> batch -> MessageBatchProcessor.process(batch, driver) }
        List<BatchPipeline.Batch<Integer>> input = batches.drop(options.warmup).indexed().collect { int index, List<String> batch ->
            new BatchPipeline.Batch<Integer>(index + options.warmup, batch)
        }
        List<Throwable> failures = Collections.synchronizedList([])
        long start = System.nanoTime()
        List<BatchPipeline.Processed<Integer>> processed = BatchPipeline.run(Flux.fromIterable(input), driver, null, 1, writers,
                { Integer token, MessageBatchResult result ->
                    if (result.batchSummary.errorCount > 0) failures << new IllegalStateException("batch ${token} had failed messages")
                } as BiConsumer,
                { Integer token, Throwable error -> failures << error } as BiConsumer, Duration.ZERO).collectList().block()
        double seconds = (System.nanoTime() - start) / 1e9d
        if (!failures.isEmpty()) throw new IllegalStateException("a server batch failed: ${failures.first()}", failures.first())
        int measured = processed.sum { it.result.batchSummary.successCount + it.result.batchSummary.errorCount } as int
        new Run(kind: "server", profile: workload.id, batch: batchSize, sessions: writers, measuredMessages: measured, seconds: seconds,
                messagesPerSecond: measured / seconds, retries: processed.sum { it.result.batchSummary.stages.retries } as int,
                nodes: countNodes(driver, workload), relationships: countRelationships(driver, workload),
                expectedRelationships: (workload.writesPerMessage - 1L) * workload.messages.size())
    }

    private static List<Neo4JNode> firstNodes(Workload workload) {
        MessageBatchProcessor.ParsedBatch parsed = MessageBatchProcessor.parse(workload.messages.take(5))
        parsed.nodeGroups.values().collect { it.prototypeNode() }
    }

    /** One phase = one write transaction, as the server does; `retries` counts the driver's transient re-runs. */
    private static void write(Session session, Prepared batch, AtomicInteger retries) {
        for (Phase phase : batch.phases) {
            int attempts = 0
            session.executeWrite({ TransactionContext tx ->
                attempts++
                for (List statement : phase.statements) {
                    tx.run((String) statement[0], (Map<String, Object>) statement[1]).consume()
                }
                null
            } as TransactionCallback<Object>)
            retries.addAndGet(Math.max(0, attempts - 1))
        }
    }

    // ---- is it the same graph?

    /**
     * Writes `verifyMessages` messages through the real server code and again through the replay, and compares a
     * fingerprint of every tagged node (label, properties except the generated Uuid and TheLinkAddedDate) and relationship.
     */
    static boolean verifyAgainstServer(String profile, Options options, Driver driver) {
        Workload sample = Workloads.build(profile, options.verifyMessages, options.seed + 7)
        cleanup(driver)
        String viaServer
        try {
            sample.messages.collate(500).each { List<String> chunk -> MessageBatchProcessor.process(chunk, driver) }
            viaServer = fingerprint(driver)
        } finally {
            cleanup(driver)
        }
        String viaReplay
        try {
            Prep prep = prepare(sample, 500)
            driver.session().withCloseable { Session session ->
                for (Neo4JNode node : firstNodes(sample)) { SchemaManager.ensure(node, session) }
                prep.batches.each { Prepared batch -> write(session, batch, new AtomicInteger()) }
            }
            viaReplay = fingerprint(driver)
        } finally {
            cleanup(driver)
        }
        viaServer == viaReplay
    }

    private static final Set<String> GENERATED = ["Uuid", "TheLinkAddedDate"] as Set

    static String fingerprint(Driver driver) {
        List<String> lines = []
        driver.session().withCloseable { Session session ->
            session.run("MATCH (n) WHERE n.${Cypher.id(Neo4JNode.keys().sourceSystem)} = \$s RETURN labels(n) AS l, properties(n) AS p", [s: Workloads.SOURCE_SYSTEM]).each { r ->
                Map props = new TreeMap(r.get("p").asMap())
                GENERATED.each { props.remove(it) }
                lines << ("N " + r.get("l").asList().sort() + " " + props).toString()
            }
            session.run("MATCH (a)-[r]->(b) WHERE a.${Cypher.id(Neo4JNode.keys().sourceSystem)} = \$s RETURN labels(a) AS al, properties(a) AS ap, type(r) AS t, properties(r) AS rp, labels(b) AS bl, properties(b) AS bp", [s: Workloads.SOURCE_SYSTEM]).each { r ->
                Map a = new TreeMap(r.get("ap").asMap()); Map b = new TreeMap(r.get("bp").asMap()); Map rel = new TreeMap(r.get("rp").asMap())
                [a, b, rel].each { Map m -> GENERATED.each { m.remove(it) } }
                lines << ("R " + r.get("al").asList().sort() + " " + a + " -[" + r.get("t").asString() + " " + rel + "]-> " + r.get("bl").asList().sort() + " " + b).toString()
            }
        }
        lines.sort()
        MessageDigest.getInstance("SHA-256").digest(lines.join("\n").getBytes("UTF-8")).encodeHex().toString() + " (" + lines.size() + " entries)"
    }

    // ---- bookkeeping

    private static long countNodes(Driver driver, Workload workload) {
        driver.session().withCloseable { Session session ->
            session.run(Cypher.versioned("MATCH (n:${Cypher.id(workload.nodeType)}) WHERE n.${Cypher.id(Neo4JNode.keys().sourceSystem)} = \$source RETURN count(n) AS c"),
                    [source: Workloads.SOURCE_SYSTEM]).single().get("c").asLong()
        }
    }

    private static long countRelationships(Driver driver, Workload workload) {
        driver.session().withCloseable { Session session ->
            session.run(Cypher.versioned("MATCH (n:${Cypher.id(workload.nodeType)})-[r]->() WHERE n.${Cypher.id(Neo4JNode.keys().sourceSystem)} = \$source RETURN count(r) AS c"),
                    [source: Workloads.SOURCE_SYSTEM]).single().get("c").asLong()
        }
    }

    private static void cleanup(Driver driver) {
        driver.session().withCloseable { Session session ->
            session.run(Cypher.versioned("MATCH (n) WHERE n.${Cypher.id(Neo4JNode.keys().sourceSystem)} = \$source CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS"),
                    [source: Workloads.SOURCE_SYSTEM]).consume()
        }
    }

    private static String report(String environment, Options options, List<Run> runs, Map<String, Prep> preps, Map<String, Boolean> equal, List<Run> comparison) {
        StringBuilder sb = new StringBuilder(environment)
        sb << "\n- Ceiling run: ${options.messages} messages per profile, batch sizes ${options.batches}, sessions ${options.sessions}, first ${options.warmup} batches are warm-up and not measured, seed ${options.seed}\n"
        sb << "\n## What this measures\n\n"
        sb << "The server's own statements and rows, in the server's lock order, replayed by N raw driver sessions (one `executeWrite` per phase) from batches that were parsed, grouped, ordered and parameterised beforehand. "
        sb << "Only the database work is timed: no parsing, no pipeline, no stage timing, no failure handling, no RabbitMQ. It is the most this database takes for this write shape on this hardware; `server % of ceiling` is how much of it the message server uses.\n"
        sb << "\n## Graph equality with the server\n\n| Profile | same nodes and relationships as the server wrote (SHA-256 of label+properties of every tagged node and relationship, `Uuid`/`TheLinkAddedDate` excluded) |\n|---|---|\n"
        equal.each { String profile, Boolean same -> sb << "| ${profile} | ${same ? 'yes' : '**NO**'} (${options.verifyMessages} messages) |\n" }
        if (comparison) {
            sb << "\n## The server against its ceiling (interleaved, ${options.repeats} repeats, batch ${options.batches.first()})\n\n"
            sb << "Each repeat runs the ceiling and then the real server pipeline (1 parser, N writers) on the same workload, one after the other, so machine state affects both alike. msg/s as median [min-max]; the last column is the median server over the median ceiling.\n\n"
            sb << "| Profile | N | ceiling (N raw sessions) | server (N writers) | server % of ceiling |\n|---|---|---|---|---|\n"
            options.profiles.each { String profile ->
                options.compare.each { int n ->
                    List<Double> c = comparison.findAll { it.profile == profile && it.sessions == n && it.kind == "ceiling" }*.messagesPerSecond
                    List<Double> v = comparison.findAll { it.profile == profile && it.sessions == n && it.kind == "server" }*.messagesPerSecond
                    if (!c || !v) return
                    double cm = Stats.percentile(c, 50), vm = Stats.percentile(v, 50)
                    sb << String.format("| %s | %d | %,.0f [%,.0f-%,.0f] | %,.0f [%,.0f-%,.0f] | %.0f%% |\n", profile, n, cm, c.min(), c.max(), vm, v.min(), v.max(), vm * 100d / cm)
                }
            }
        }
        sb << "\n## Ceiling, msg/s\n\n| Profile | batch | " + options.sessions.collect { "${it} session${it == 1 ? '' : 's'}" }.join(" | ") + " | retries |\n|---|---|" + "---|" * options.sessions.size() + "---|\n"
        options.profiles.each { String profile ->
            options.batches.each { int batch ->
                List<Run> row = runs.findAll { it.profile == profile && it.batch == batch }
                sb << "| ${profile} | ${batch} | " + options.sessions.collect { int s -> row.find { it.sessions == s }?.with { String.format("%,.0f", it.messagesPerSecond) } ?: "-" }.join(" | ") + " | ${row*.retries.sum() ?: 0} |\n"
            }
        }
        sb << "\n## What the application does around the database, per message (microseconds, single thread)\n\n"
        sb << "| Profile | batch | parse+group | lock ordering | parameter maps | app total | single-thread app ceiling msg/s |\n|---|---|---|---|---|---|---|\n"
        options.profiles.each { String profile ->
            options.batches.each { int batch ->
                Prep p = preps["${profile}/${batch}".toString()]
                double total = p.parseGroupMicrosPerMessage + p.orderMicrosPerMessage + p.parametersMicrosPerMessage
                sb << String.format("| %s | %d | %.1f | %.1f | %.1f | %.1f | %,.0f |\n", profile, batch, p.parseGroupMicrosPerMessage, p.orderMicrosPerMessage, p.parametersMicrosPerMessage, total, 1_000_000d / total)
            }
        }
        sb << "\nThe server runs the three on its parser and writer threads, so the app side only limits throughput when its single-thread ceiling (or P parsers' worth of it) is below the database ceiling for the same number of sessions. Not included: schema checks per batch, session handling, metrics, RabbitMQ read and ack.\n"
        sb.toString()
    }
}
