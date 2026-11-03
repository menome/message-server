package com.menome.benchmark

import com.menome.MessagingWithTestContainersSpecification
import com.menome.messageBatchProcessor.MessageBatchProcessor
import com.menome.messageBatchProcessor.SchemaManager
import com.menome.util.Neo4J
import org.neo4j.driver.Driver

/**
 * KANBAN E10-1: the ceiling replays the server's statements without the server around them. Only worth anything if it
 * writes the same graph, so that is what is checked here, against a real Neo4j, including that the check can fail.
 */
class CeilingRunnerSpecification extends MessagingWithTestContainersSpecification {

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
        driver.session().run("MATCH (n) WHERE n.SourceSystem = '${Workloads.SOURCE_SYSTEM}' DETACH DELETE n").consume()
    }

    private CeilingRunner.Options options() {
        CeilingRunner.Options options = new CeilingRunner.Options()
        options.serverHardware = "test container"
        options.verifyMessages = 300
        options
    }

    def "the replay writes the same graph as the server for #profile"() {
        expect:
        CeilingRunner.verifyAgainstServer(profile, options(), driver)

        where:
        profile << Workloads.ids()
    }

    def "the fingerprint changes when a property or a relationship differs, so the equality check can fail"() {
        given:
        Workloads.build("hubs3", 100, 5).messages.collate(50).each { MessageBatchProcessor.process(it, driver) }
        String before = CeilingRunner.fingerprint(driver)

        expect: "writing the same graph again is the same fingerprint (Uuid and the added date are left out)"
        CeilingRunner.fingerprint(driver) == before

        when: "one property differs"
        driver.session().run("MATCH (n:BenchNode {BenchId: 3}) SET n.Prop3 = -1").consume()
        String changedProperty = CeilingRunner.fingerprint(driver)

        and: "and then one relationship is missing"
        driver.session().run("MATCH (n:BenchNode {BenchId: 3}) SET n.Prop3 = n.Prop3 + 0").consume()
        driver.session().run("MATCH (:BenchNode {BenchId: 4})-[r]->() WITH r LIMIT 1 DELETE r").consume()
        String missingRelationship = CeilingRunner.fingerprint(driver)

        then:
        changedProperty != before
        missingRelationship != changedProperty
    }

    def "a ceiling run over a small workload writes every node and relationship and reports success"() {
        given:
        CeilingRunner.Options options = options()
        options.profiles = ["hubs3", "unique-dest"]
        options.messages = 1500
        options.warmup = 1
        options.batches = [250, 500]
        options.sessions = [1, 3]
        options.batch = 250

        expect: "exit status 0 means node and relationship counts matched on every run"
        CeilingRunner.run(options) == 0
    }
}
