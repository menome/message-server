package com.menome.benchmark

import com.menome.message.Neo4JConnection
import com.menome.message.Neo4JNode
import com.menome.messageBatchProcessor.MessageBatchCollection
import com.menome.messageBatchProcessor.MessageConnectionCollection
import com.menome.messageParser.ConvertJSonToNeo4JObjects
import com.menome.util.Cypher
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.Transaction
import org.neo4j.driver.summary.ProfiledPlan

/**
 * Neo4j-side evidence for a workload (KANBAN E1-4): the statement MessageBatchProcessor generates for every signature
 * in the first batch, run under PROFILE so the report shows the plan, db hits, rows and page cache hits per operator.
 *
 * Profiling is rolled back, so the graph is untouched and the same batch can be profiled twice: once with the
 * messages already written (every MERGE matches) and once with never-seen keys (every primary MERGE creates).
 *
 * Page cache hits/misses per operator are included when Neo4j reports them (Community does not). Server-wide page cache
 * ratio and lock-wait counters are not here either: Community exposes no metrics over Bolt (queryJmx returns no beans,
 * there are no server.metrics.* settings). Lock contention shows up as `retries` instead.
 */
class PlanReport {

    static class StatementPlan {
        String kind              // node or relationship
        String signature
        String cypher
        int rows
        long dbHits
        long pageCacheHits
        long pageCacheMisses
        boolean hasPageCacheStats
        List<String> scans = []
        String plan

        double dbHitsPerRow() { rows ? dbHits / (double) rows : 0d }

        /** Percent of page cache accesses that were hits, or null when Neo4j did not report them. */
        Double pageCacheHitPercent() {
            long total = pageCacheHits + pageCacheMisses
            hasPageCacheStats && total > 0 ? pageCacheHits * 100d / total : null
        }
    }

    /**
     * Plans for every node and relationship signature found in the first `batchSize` messages, in the order the
     * processor runs them (nodes, then relationships). All statements run in ONE transaction that is rolled back, so
     * relationship statements see the nodes written just before them and the graph is left untouched.
     */
    static List<StatementPlan> profile(Driver driver, Workload workload, int batchSize) {
        Map<String, MessageBatchCollection> nodes = [:]
        Map<String, MessageConnectionCollection> connections = [:]
        workload.messages.take(batchSize).eachWithIndex { String message, int index ->
            def (List<Neo4JNode> n, List<Neo4JConnection> c) = ConvertJSonToNeo4JObjects.fromJson(message, index)
            n.each { nodes.computeIfAbsent(it.signature(), { sig -> new MessageBatchCollection(sig) }).addNode(it) }
            c.each { connections.computeIfAbsent(it.signature(), { sig -> new MessageConnectionCollection(sig) }).addConnection(it) }
        }
        List<StatementPlan> plans = []
        driver.session().withCloseable { Session session ->
            Transaction tx = session.beginTransaction()
            try {
                nodes.values().each { group ->
                    plans << profileStatement(tx, "node", group.signature, group.prototypeNode().toCypher(), MessageBatchCollection.asParameterList(group.nodeList()), group.nodes.size())
                }
                connections.values().each { group ->
                    plans << profileStatement(tx, "relationship", group.signature, group.prototypeRelationship().toCypher(), MessageConnectionCollection.asParameterList(group.connectionList()), group.connections.size())
                }
            } finally {
                tx.rollback()
                tx.close()
            }
        }
        plans
    }

    private static StatementPlan profileStatement(Transaction tx, String kind, String signature, String cypher, Map parameters, int rows) {
        ProfiledPlan profiled = tx.run(Cypher.versioned("PROFILE UNWIND \$parms AS param " + cypher), parameters).consume().profile()
        StatementPlan plan = new StatementPlan(kind: kind, signature: signature, cypher: cypher, rows: rows)
        StringBuilder text = new StringBuilder()
        walk(profiled, 0, plan, text)
        plan.plan = text.toString()
        plan
    }

    private static void walk(ProfiledPlan node, int depth, StatementPlan totals, StringBuilder text) {
        totals.dbHits += node.dbHits()
        if (node.hasPageCacheStats()) {
            totals.hasPageCacheStats = true
            totals.pageCacheHits += node.pageCacheHits()
            totals.pageCacheMisses += node.pageCacheMisses()
        }
        String operator = node.operatorType()
        if (operator.contains("Scan")) totals.scans << operator
        String details = node.arguments().get("Details")?.asString("") ?: ""
        text << "  " * depth << "+" << operator << "  rows=" << node.records() << " dbHits=" << node.dbHits()
        if (node.hasPageCacheStats()) text << " pageCache=" << node.pageCacheHits() << "/" << node.pageCacheMisses()
        if (details) text << "  " << (details.length() > 110 ? details.substring(0, 107) + "..." : details)
        text << "\n"
        node.children().each { walk(it, depth + 1, totals, text) }
    }

    /** Markdown: a summary table, then the full plan of each statement in a collapsible block. */
    static String markdown(String title, List<StatementPlan> plans) {
        StringBuilder sb = new StringBuilder()
        sb << "**${title}**\n\n"
        sb << "| Kind | Rows | db hits | db hits/row | page cache hit % | scans | Statement |\n|---|---|---|---|---|---|---|\n"
        plans.each { StatementPlan p ->
            Double hit = p.pageCacheHitPercent()
            sb << String.format("| %s | %,d | %,d | %.1f | %s | %s | `` %s `` |\n", p.kind, p.rows, p.dbHits, p.dbHitsPerRow(),
                    hit == null ? "n/a" : String.format("%.1f", hit), p.scans ? p.scans.join(", ") : "none", oneLine(p.cypher).replace("|", "\\|"))
        }
        plans.each { StatementPlan p ->
            sb << "\n<details><summary>${p.kind} plan: ${oneLine(p.cypher).take(90).replace("<", "&lt;").replace(">", "&gt;")}</summary>\n\n```\n${p.plan}```\n\n</details>\n"
        }
        sb.toString()
    }

    private static String oneLine(String cypher) {
        cypher.replaceAll(/\s+/, " ").trim()
    }
}
