package com.menome.benchmark

import com.menome.MessagingWithTestContainersSpecification
import com.menome.messageBatchProcessor.MessageBatchProcessor
import com.menome.util.Neo4J
import org.neo4j.driver.Driver

class PlanReportSpecification extends MessagingWithTestContainersSpecification {

    Driver driver

    def setup() {
        driver = Neo4J.openDriver()
        clean()
    }

    def cleanup() {
        clean()
        driver.close()
    }

    private void clean() {
        driver.session().run("MATCH (n) WHERE n.SourceSystem = '${Workloads.SOURCE_SYSTEM}' DETACH DELETE n").consume()
    }

    private long count(String label) {
        driver.session().run("MATCH (n:${label}) RETURN count(n) AS c").single().get("c").asLong()
    }

    def "there is a plan for every signature in the batch, nodes before relationships"() {
        given:
        Workload workload = Workloads.build("hubs3", 200, 1)
        MessageBatchProcessor.process(workload.messages.take(50), driver) // creates the indexes

        when:
        List<PlanReport.StatementPlan> plans = PlanReport.profile(driver, workload, 100)

        then:
        // primary + office + project + team nodes, then three relationship types
        plans*.kind == ["node", "node", "node", "node", "relationship", "relationship", "relationship"]
        plans.every { it.dbHits > 0 && it.plan.contains("+") && it.cypher }
        plans.findAll { it.kind == "node" }.first().rows == 100
    }

    def "profiling is rolled back and leaves the graph untouched"() {
        given:
        Workload workload = Workloads.build("hubs3", 100, 1)

        when:
        PlanReport.profile(driver, workload, 100)

        then:
        count("BenchNode") == 0
        count("BenchOffice") == 0
    }

    def "relationship plans see the nodes written earlier in the same batch"() {
        when:
        List<PlanReport.StatementPlan> plans = PlanReport.profile(driver, Workloads.build("hubs3", 100, 1), 100)

        then:
        // If nodes were not visible to the relationship statements the MATCH would find nothing and cost nothing
        plans.findAll { it.kind == "relationship" }.every { it.dbHits >= 100 }
    }

    def "once the index exists no statement needs a scan"() {
        given:
        Workload workload = Workloads.build("wide", 200, 1)
        MessageBatchProcessor.process(workload.messages.take(50), driver)

        when:
        List<PlanReport.StatementPlan> plans = PlanReport.profile(driver, workload, 100)

        then:
        plans.every { it.scans.isEmpty() }
    }

    def "the markdown has a table row and a collapsible plan per statement"() {
        given:
        Workload workload = Workloads.build("hubs3", 50, 1)
        MessageBatchProcessor.process(workload.messages, driver)

        when:
        List<PlanReport.StatementPlan> plans = PlanReport.profile(driver, workload, 50)
        String markdown = PlanReport.markdown("Title", plans)

        then:
        markdown.startsWith("**Title**")
        markdown.count("<details>") == plans.size()
        markdown.contains("WORKS_IN")
        // arrows in Cypher must not be read as HTML in the summary line
        !markdown.readLines().findAll { it.startsWith("<details>") }.any { it.contains("<-") }
    }

    // Keep this feature last: it drops a constraint that SchemaManager remembers having created in this JVM
    def "a statement without an index is reported as scanning"() {
        given:
        MessageBatchProcessor.process(Workloads.build("node-only", 1, 1).messages, driver)
        // The node type is backed by a uniqueness constraint (and its index); without it MERGE has to scan the label
        String constraintName = driver.session().run("SHOW CONSTRAINTS YIELD name, labelsOrTypes WHERE 'BenchNode' IN labelsOrTypes RETURN name LIMIT 1").single().get("name").asString()
        driver.session().run("DROP CONSTRAINT `${constraintName}`").consume()
        clean()

        when:
        List<PlanReport.StatementPlan> plans = PlanReport.profile(driver, Workloads.build("node-only", 20, 1), 20)

        then:
        plans.first().scans.any { it.contains("Scan") }
    }
}
