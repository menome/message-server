package com.menome.messageBatchProcessor

import com.menome.MessagingWithTestContainersSpecification
import com.menome.benchmark.Workload
import com.menome.benchmark.Workloads
import com.menome.util.Neo4J
import com.menome.util.PreferenceType
import org.neo4j.driver.Driver
import reactor.core.publisher.Flux

import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.function.BiConsumer

/** KANBAN E4-2 against a real Neo4j: several writers on nodes they all share must lose nothing, duplicate nothing and not give up. */
class ParallelWritersSpecification extends MessagingWithTestContainersSpecification {

    Driver driver

    def setup() {
        driver = Neo4J.openDriver()
        clean()
    }

    def cleanup() {
        clean()
        System.clearProperty(PreferenceType.RELATIONSHIP_TRANSACTIONS.name())
        driver.close()
    }

    private void clean() {
        driver.session().run("MATCH (n) WHERE n.SourceSystem = '${Workloads.SOURCE_SYSTEM}' DETACH DELETE n").consume()
    }

    private long count(String cypher) {
        driver.session().run(cypher).single().get(0).asLong()
    }

    /** Runs the workload through the pipeline and returns [batches that failed, error counts]. */
    private Map runPipeline(Workload workload, int parsers, int writers, int batchSize = 200) {
        List failures = new CopyOnWriteArrayList()
        List<Integer> failedMessages = new CopyOnWriteArrayList<>()
        List<BatchPipeline.Batch<Integer>> input = workload.messages.collate(batchSize).indexed().collect { int i, List<String> m -> new BatchPipeline.Batch<Integer>(i, m) }
        List<BatchPipeline.Processed<Integer>> processed = BatchPipeline.run(Flux.fromIterable(input), driver, null, parsers, writers,
                { Integer token, MessageBatchResult result -> failedMessages << result.batchSummary.errorCount } as BiConsumer,
                { Integer token, Throwable failure -> failures << failure } as BiConsumer, Duration.ZERO).collectList().block(Duration.ofSeconds(120))
        [failures: failures, failedMessages: failedMessages.sum(0), processed: processed.size(), batches: input.size()]
    }

    def "four writers on messages that share accounts and activities write every message once, with no failures (#scope)"() {
        given: "wide: 1 node + 3 relationships into pools of 5000 / 10 / 5000 shared nodes, so batches overlap heavily"
        System.setProperty(PreferenceType.RELATIONSHIP_TRANSACTIONS.name(), scope)
        Workload workload = Workloads.build("wide", 6000, 7)

        when:
        Map outcome = runPipeline(workload, 2, 4)

        then:
        outcome.failures.isEmpty()
        outcome.failedMessages == 0
        outcome.processed == outcome.batches
        count("MATCH (n:BenchNode) RETURN count(n)") == 6000
        count("MATCH (n:BenchNode) RETURN count(DISTINCT n.BenchId)") == 6000
        // one relationship of each type per message, none lost, none duplicated
        ["ENTRY_FOR_ACCOUNT", "ACTIVITY", "DIALER"].every { String type -> count("MATCH (:BenchNode)-[r:`${type}`]->() RETURN count(r)") == 6000 }
        // shared nodes exist once each: the number of distinct destinations used by the messages
        count("MATCH (n:BenchAccount) RETURN count(n)") == count("MATCH (n:BenchAccount) RETURN count(DISTINCT n.ACCTNUM)")
        count("MATCH (n:BenchDialer) RETURN count(n)") == count("MATCH (n:BenchDialer) RETURN count(DISTINCT [n.ACCTNUM, n.SNAPSHOT])")

        where:
        scope << ["phase", "statement"]
    }

    def "four writers all pointing at the same three hubs: every relationship exists and there are exactly three hubs of each kind"() {
        given:
        Workload workload = Workloads.build("hubs3", 4000, 3)

        when:
        Map outcome = runPipeline(workload, 1, 4)

        then:
        outcome.failures.isEmpty()
        count("MATCH (n:BenchNode) RETURN count(n)") == 4000
        ["BenchOffice", "BenchProject", "BenchTeam"].every { count("MATCH (n:`${it}`) RETURN count(n)") == 3 }
        ["WORKS_IN", "WORKS_ON", "MEMBER_OF"].every { String type -> count("MATCH (:BenchNode)-[r:`${type}`]->() RETURN count(r)") == 4000 }
    }

    def "a re-delivered batch (at-least-once) written by two writers at once changes nothing"() {
        given: "the same 500 messages arrive twice and are written concurrently"
        Workload workload = Workloads.build("hubs3", 500, 5)
        Workload twice = new Workload(id: "twice", nodeType: workload.nodeType, writesPerMessage: workload.writesPerMessage, messages: workload.messages + workload.messages)

        when:
        Map outcome = runPipeline(twice, 1, 2, 500)

        then:
        outcome.failures.isEmpty()
        count("MATCH (n:BenchNode) RETURN count(n)") == 500
        count("MATCH (:BenchNode)-[r:WORKS_IN]->() RETURN count(r)") == 500
    }
}
