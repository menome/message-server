package com.menome.messageBatchProcessor

import com.google.gson.Gson
import com.menome.MessagingWithTestContainersSpecification
import com.menome.benchmark.PlanReport
import com.menome.benchmark.Workload
import com.menome.benchmark.Workloads
import com.menome.message.Neo4JNode
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.neo4j.driver.exceptions.ClientException

import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** KANBAN E3-1 against a real Neo4j: constraints, migration from the old plain index, duplicates, concurrent writers. */
class SchemaConstraintSpecification extends MessagingWithTestContainersSpecification {

    static final String LABEL = "SchemaTestNode"

    Driver driver

    def setup() {
        driver = Neo4J.openDriver()
        clean()
        SchemaManager.forgetAll()
    }

    def cleanup() {
        clean()
        SchemaManager.forgetAll()
        driver.close()
    }

    private void clean() {
        driver.session().run("MATCH (n) WHERE n.SourceSystem = '${Workloads.SOURCE_SYSTEM}' AND any(l IN labels(n) WHERE l STARTS WITH 'SchemaTest') DETACH DELETE n").consume()
        driver.session().run("SHOW CONSTRAINTS YIELD name, labelsOrTypes WHERE labelsOrTypes[0] STARTS WITH 'SchemaTest' RETURN name").list().each {
            driver.session().run("DROP CONSTRAINT `${it.get('name').asString()}` IF EXISTS").consume()
        }
        driver.session().run("SHOW INDEXES YIELD name, type, labelsOrTypes WHERE type = 'RANGE' AND labelsOrTypes[0] STARTS WITH 'SchemaTest' RETURN name").list().each {
            driver.session().run("DROP INDEX `${it.get('name').asString()}` IF EXISTS").consume()
        }
    }

    private static String message(long id, String label = LABEL) {
        new Gson().toJson([Name: "n${id}".toString(), NodeType: label, Priority: 1, SourceSystem: Workloads.SOURCE_SYSTEM, ConformedDimensions: [Id: id]])
    }

    private List<Map> constraintsOn(String label = LABEL) {
        driver.session().run("SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties WHERE labelsOrTypes = [\$label] RETURN type, properties", [label: label]).list { it.asMap() }
    }

    private List<Map> plainIndexesOn(String label = LABEL) {
        driver.session().run("SHOW INDEXES YIELD name, type, labelsOrTypes, owningConstraint WHERE type = 'RANGE' AND labelsOrTypes = [\$label] AND owningConstraint IS NULL RETURN name", [label: label]).list { it.asMap() }
    }

    private long count(String label = LABEL) {
        driver.session().run("MATCH (n:`${label}`) RETURN count(n) AS c").single().get("c").asLong()
    }

    def "the first batch for a node type creates a uniqueness constraint on its dimension keys, and MERGE seeks on it"() {
        when:
        MessageBatchResult result = MessageBatchProcessor.process((1..20).collect { message(it) }, driver)

        then:
        result.batchSummary.errorCount == 0
        constraintsOn() == [[type: "NODE_PROPERTY_UNIQUENESS", properties: ["Id"]]]
        plainIndexesOn().isEmpty()

        when:
        List<PlanReport.StatementPlan> plans = PlanReport.profile(driver,
                new Workload(id: "schema", nodeType: LABEL, messages: (21..40).collect { message(it) }, writesPerMessage: 1), 20)

        then:
        // Under a uniqueness constraint Neo4j plans MERGE as one dedicated operator: a unique index lookup plus create
        plans.first().plan.contains("MergeUniqueNode")
        plans.first().scans.isEmpty()
    }

    def "a duplicate cannot be created behind the message server's back, and the error is classified as bad data"() {
        given:
        MessageBatchProcessor.process([message(1)], driver)

        when:
        driver.session().run("CREATE (:`${LABEL}` {Id: 1})").consume()

        then:
        ClientException e = thrown()
        e.code().startsWith("Neo.ClientError.Schema.")
        MessageBatchProcessor.isDataError(e)
    }

    def "a plain index from an earlier version is replaced by the constraint and the data is still written"() {
        given: "what the previous release left behind"
        driver.session().run(new Neo4JNode(nodeType: LABEL, conformedDimensions: [Id: 1]).indexCypher()).consume()
        assert plainIndexesOn().size() == 1

        when:
        MessageBatchResult result = MessageBatchProcessor.process((1..10).collect { message(it) }, driver)

        then:
        result.batchSummary.errorCount == 0
        constraintsOn().size() == 1
        plainIndexesOn().isEmpty()
        count() == 10
    }

    def "KEY_CONSTRAINT=nodekey on an edition without node keys keeps the uniqueness constraint it has, untouched"() {
        given:
        MessageBatchProcessor.process((1..5).collect { message(it) }, driver)
        String before = driver.session().run("SHOW CONSTRAINTS YIELD name, labelsOrTypes WHERE labelsOrTypes = ['${LABEL}'] RETURN name").single().get("name").asString()
        SchemaManager.forgetAll()
        System.setProperty(com.menome.util.PreferenceType.KEY_CONSTRAINT.name(), "nodekey")

        when:
        MessageBatchResult result = MessageBatchProcessor.process((6..10).collect { message(it) }, driver)

        then:
        result.batchSummary.errorCount == 0
        // same constraint, same name: it was not dropped and recreated (which would rebuild its index for nothing)
        driver.session().run("SHOW CONSTRAINTS YIELD name, type, labelsOrTypes WHERE labelsOrTypes = ['${LABEL}'] RETURN name, type").list { it.asMap() } == [[name: before, type: "NODE_PROPERTY_UNIQUENESS"]]
        count() == 10

        cleanup:
        System.clearProperty(com.menome.util.PreferenceType.KEY_CONSTRAINT.name())
    }

    def "a graph that already holds duplicates keeps working on a plain index, loudly, and without a constraint"() {
        given:
        driver.session().run("CREATE (:`${LABEL}` {Id: 1, SourceSystem: '${Workloads.SOURCE_SYSTEM}'}), (:`${LABEL}` {Id: 1, SourceSystem: '${Workloads.SOURCE_SYSTEM}'})").consume()

        when:
        MessageBatchResult result = MessageBatchProcessor.process((1..5).collect { message(it) }, driver)

        then:
        noExceptionThrown()
        result.batchSummary.errorCount == 0
        constraintsOn().isEmpty()
        plainIndexesOn().size() == 1
        count() == 6   // the two existing duplicates plus ids 2..5; MERGE on Id=1 matched them instead of adding a third
    }

    def "writers working on overlapping keys at the same time never create a duplicate node"() {
        given:
        int writers = 4
        int batchesPerWriter = 25
        Set<Long> seen = ConcurrentHashMap.newKeySet()
        MessageBatchProcessor.process([message(0)], driver)   // schema created up front, as it would be in a running server
        def pool = Executors.newFixedThreadPool(writers)

        when:
        List futures = (1..writers).collect { int writer ->
            pool.submit({
                Random random = new Random(writer)
                batchesPerWriter.times {
                    // Sorted, so concurrent transactions lock keys in the same order and do not deadlock each other
                    List<Long> ids = (1..100).collect { (long) random.nextInt(300) }.unique().sort()
                    seen.addAll(ids)
                    MessageBatchResult result = MessageBatchProcessor.process(ids.collect { message(it) }, driver)
                    assert result.batchSummary.errorCount == 0
                }
            } as Callable)
        }
        futures*.get(120, TimeUnit.SECONDS)
        pool.shutdown()
        def row = driver.session().run("MATCH (n:`${LABEL}`) RETURN count(n) AS total, count(DISTINCT n.Id) AS distinctIds").single()

        then:
        row.get("total").asLong() == row.get("distinctIds").asLong()
        row.get("distinctIds").asLong() == seen.size() + (seen.contains(0L) ? 0 : 1)
    }
}
