package com.menome.benchmark

import com.menome.messageBatchProcessor.MessageBatchProcessor
import com.menome.messageBatchProcessor.MessageBatchResult
import com.menome.util.Cypher
import com.menome.util.Neo4J
import com.menome.message.Neo4JNode
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import org.neo4j.driver.Driver

/**
 * KANBAN E3-14: what one bad message costs a batch. The bad message has a nested object where its siblings have a number,
 * so it shares their statement (same keys) and only Neo4j can reject it: the phase rolls back and is bisected.
 * Also reports the size of the messages, for the unacked-message memory budget (prefetch = batch x (parsers + writers + 1)).
 *   NEO4J_BOLT_PORT=7688 ./gradlew bisect --args="--profiles=node-only,hubs3 --batches=500,2000,5000 --repeats=5"
 */
class BisectCost {

    static void main(String[] args) {
        Map<String, String> opts = args.collectEntries { String a -> a.replaceFirst("^--", "").split("=", 2).with { [(it[0]): it[1]] } }
        List<String> profiles = (opts.profiles ?: "node-only,hubs3").split(",").toList()
        List<Integer> batches = (opts.batches ?: "500,2000,5000").split(",").collect { it as int }
        int repeats = (opts.repeats ?: "5") as int
        Driver driver = Neo4J.openDriver()
        try {
            driver.verifyConnectivity()
            println "| profile | batch | bytes/msg | unacked MB at 1 writer / 4 writers | clean ms | 1 bad message ms | extra | failed | committed |"
            println "|---|---|---|---|---|---|---|---|---|"
            profiles.each { String profile ->
                batches.each { int size ->
                    cleanup(driver)
                    List<String> clean = Workloads.build(profile, size, 42).messages
                    List<String> bad = new ArrayList<>(clean)
                    int at = (int) (size * 0.37)
                    JsonObject m = JsonParser.parseString(bad[at]).asJsonObject
                    JsonObject properties = m.getAsJsonObject("Properties")
                    properties.add(properties.keySet().first(), JsonParser.parseString('{"nested": 1}'))
                    bad[at] = m.toString()
                    MessageBatchProcessor.process(clean, driver) // creates the nodes; later runs match them, for both variants
                    List<Long> cleanMs = [], badMs = []
                    MessageBatchResult last = null
                    repeats.times {
                        cleanMs << time { MessageBatchProcessor.process(clean, driver) }
                        badMs << time { last = MessageBatchProcessor.process(bad, driver) }
                    }
                    double bytes = clean.sum { it.getBytes("UTF-8").length } / size
                    long c = median(cleanMs), b = median(badMs)
                    println "| ${profile} | ${size} | ${Math.round(bytes)} | ${bytesMb(bytes, size, 3)} / ${bytesMb(bytes, size, 6)} | ${c} | ${b} | ${b - c} ms (${Math.round(b / (double) c * 10) / 10}x) | ${last.batchSummary.errorCount} | ${last.batchSummary.successCount} |"
                }
            }
            cleanup(driver)
        } finally {
            driver.close()
        }
    }

    private static String bytesMb(double bytes, int size, int factor) { String.format("%.1f", bytes * size * factor / 1e6) }

    private static long time(Closure c) { long t = System.nanoTime(); c(); (System.nanoTime() - t) / 1_000_000 as long }

    private static long median(List<Long> xs) { xs.sort(false)[xs.size().intdiv(2)] }

    private static void cleanup(Driver driver) {
        driver.session().withCloseable { s ->
            s.run(Cypher.versioned("MATCH (n) WHERE n.${Cypher.id(Neo4JNode.keys().sourceSystem)} = \$source CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS"),
                    [source: Workloads.SOURCE_SYSTEM]).consume()
        }
    }
}
