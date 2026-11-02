package com.menome.messageBatchProcessor

import com.menome.benchmark.Workloads
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.neo4j.driver.Driver
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionCallback
import org.neo4j.driver.TransactionContext
import org.neo4j.driver.summary.ResultSummary
import spock.lang.Specification

/** Transient retries are counted from how often executeWrite calls the statement; no database needed. */
class RetryCountSpecification extends Specification {

    private Driver driverThatRuns(int attemptsPerStatement) {
        Session session = Mock()
        session.executeWrite(_) >> { List args ->
            TransactionCallback callback = args[0]
            TransactionContext tx = Mock()
            Result result = Mock()
            tx.run(_, _) >> result
            result.consume() >> Mock(ResultSummary)
            // The driver calls the callback again after a transient error; emulate that
            (1..attemptsPerStatement).each { callback.execute(tx) }
            null
        }
        Driver driver = Mock()
        driver.session() >> session
        driver
    }

    def "no retries when every statement succeeds first time"() {
        when:
        MessageBatchResult result = MessageBatchProcessor.process(Workloads.build("node-only", 10, 1).messages, driverThatRuns(1))

        then:
        result.batchSummary.stages.retries == 0
    }

    def "every extra attempt of a transaction is a retry"() {
        when:
        // hubs3 = 2 write transactions (all node statements, then all relationship statements), each attempted three times
        MessageBatchResult result = MessageBatchProcessor.process(Workloads.build("hubs3", 10, 1).messages, driverThatRuns(3))

        then:
        result.batchSummary.stages.retries == 2 * 2
        result.batchSummary.stages.transactions == 2
    }

    def "retries reach the registry as messageserver.write.retries"() {
        given:
        SimpleMeterRegistry registry = new SimpleMeterRegistry()

        when:
        MessageBatchProcessor.process(Workloads.build("node-only", 10, 1).messages, driverThatRuns(2), registry)

        then:
        registry.get("messageserver.write.retries").counter().count() == 1d
    }
}
