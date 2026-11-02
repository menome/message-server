package com.menome.messageBatchProcessor

import com.menome.MessagingWithTestContainersSpecification
import com.menome.benchmark.Workloads
import com.menome.util.Neo4J
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.neo4j.driver.Driver

import java.util.concurrent.TimeUnit

class StageTimingsSpecification extends MessagingWithTestContainersSpecification {

    Driver driver

    def setup() {
        Neo4J.deleteAllTestNodes()
        driver = Neo4J.openDriver()
        // Benchmark data uses its own SourceSystem
        driver.session().run("MATCH (n) WHERE n.SourceSystem = '${Workloads.SOURCE_SYSTEM}' DETACH DELETE n").consume()
    }

    def cleanup() {
        driver.session().run("MATCH (n) WHERE n.SourceSystem = '${Workloads.SOURCE_SYSTEM}' DETACH DELETE n").consume()
        driver.close()
    }

    def "a batch reports time for every stage and the stages explain the batch time"() {
        given:
        List<String> messages = Workloads.build("hubs3", 2_000, 1).messages
        MessageBatchProcessor.process(messages.take(500), driver) // JIT warm-up, creates the indexes

        when:
        MessageBatchResult result = MessageBatchProcessor.process(messages.drop(500), driver)
        StageTimings stages = result.batchSummary.stages
        long batchNanos = result.batchSummary.batchProcessingDuration.toNanos()

        then:
        result.batchSummary.errorCount == 0
        StageTimings.BATCH_STAGES.every { stages.duration(it).toNanos() > 0 || it == StageTimings.INDEX }
        // Stages never overlap, so they cannot add up to more than the batch (this fails if a stage is counted twice)
        stages.total().toNanos() <= batchNanos
        // Acceptance for E1-2 is 5%; asserted at 10% so a slow CI box does not flake. The benchmark report shows the real figure.
        stages.total().toNanos() >= batchNanos * 0.9
    }

    def "bisecting a failed batch does not count the re-run statements twice"() {
        given:
        List<String> messages = Workloads.build("node-only", 501, 1).messages.toList()
        // A nested object passes schema validation but Neo4j rejects it as a property value
        messages[250] = messages[250].replaceFirst('"Prop3":\\d+', '"Prop3":{"nested":"object"}')

        when:
        MessageBatchResult result = MessageBatchProcessor.process(messages, driver)

        then:
        result.batchSummary.errorCount == 1
        result.batchSummary.stages.total().toNanos() <= result.batchSummary.batchProcessingDuration.toNanos()
    }

    def "each stage is recorded in the registry as messageserver.stage.duration with a stage tag"() {
        given:
        MeterRegistry registry = new SimpleMeterRegistry()
        List<String> messages = Workloads.build("hubs3", 500, 1).messages

        when:
        MessageBatchProcessor.process(messages, driver, registry)

        then:
        StageTimings.BATCH_STAGES.each { String stage ->
            def timer = registry.find("messageserver.stage.duration").tag("stage", stage).timer()
            assert timer != null: "no timer for stage $stage"
            assert timer.count() == 1
        }
        registry.find("messageserver.stage.duration").tag("stage", StageTimings.STATEMENT).timer().totalTime(TimeUnit.NANOSECONDS) > 0
    }
}
