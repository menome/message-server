package com.menome.messageBatchProcessor

import com.menome.benchmark.Workloads
import com.menome.util.PreferenceType
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.exceptions.Neo4jException
import reactor.core.publisher.Flux
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

import static org.awaitility.Awaitility.await

/**
 * KANBAN E5-3, E5-6, E3-12 against a real 3-primary Enterprise cluster. Needs `docker-compose.cluster.yml` to be up and
 * RUN_CLUSTER_TESTS=Y; skipped otherwise, so the normal suite does not need a cluster:
 *
 *   docker compose -f docker-compose.cluster.yml up -d --wait
 *   RUN_CLUSTER_TESTS=Y ./gradlew test --tests '*ClusterSpecification'
 *
 * The failover tests stop or kill the leader's container and start it again afterwards.
 */
@Requires({ env.RUN_CLUSTER_TESTS == "Y" })
class ClusterSpecification extends Specification {

    static final List<Integer> PORTS = [7687, 7688, 7689]   // server1..3, as published by the compose file
    static final String PASSWORD = "password"

    @Shared Driver routing

    def setupSpec() {
        System.setProperty("NEO4J_HOST", "neo4j://localhost")
        System.setProperty("NEO4J_BOLT_PORT", "7687")
        routing = GraphDatabase.driver("neo4j://localhost:7687", AuthTokens.basic("neo4j", PASSWORD))
        awaitClusterFormed()
    }

    def cleanupSpec() {
        routing?.close()
        System.clearProperty("NEO4J_HOST")
        System.clearProperty("NEO4J_BOLT_PORT")
    }

    def setup() {
        SchemaManager.forgetAll()
        cleanData()
        dropBenchSchema()
    }

    def cleanup() {
        System.clearProperty(PreferenceType.KEY_CONSTRAINT.name())
        cleanData()
        dropBenchSchema()
        SchemaManager.forgetAll()
    }

    // ---- helpers

    private static Driver direct(int port) {
        GraphDatabase.driver("bolt://localhost:${port}", AuthTokens.basic("neo4j", PASSWORD))
    }

    /** Port of the member that currently accepts writes for the neo4j database. */
    private int leaderPort() {
        Driver any = direct(PORTS[0])
        try {
            String address = any.session().run("SHOW DATABASES YIELD name, address, writer WHERE name = 'neo4j' AND writer RETURN address").single().get("address").asString()
            address.split(":")[1] as int
        } finally {
            any.close()
        }
    }

    private static String containerFor(int port) { "cluster-server${port - 7686}" }

    private static void docker(String... args) {
        Process process = new ProcessBuilder(["docker"] + args.toList()).redirectErrorStream(true).start()
        String output = process.inputStream.text
        assert process.waitFor() == 0: "docker ${args.join(' ')} failed: ${output}"
    }

    /** At least 3 members report the neo4j database online; tolerant of members that are still starting. */
    private void awaitClusterFormed() {
        await().atMost(180, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).ignoreExceptions().until {
            Driver d = direct(PORTS.find { true })
            try {
                d.session().run("SHOW DATABASES YIELD name, currentStatus WHERE name = 'neo4j' AND currentStatus = 'online' RETURN count(*) AS c").single().get("c").asLong() >= 3
            } finally {
                d.close()
            }
        }
    }

    private void cleanData() {
        routing.session().run("MATCH (n) WHERE n.SourceSystem = '${Workloads.SOURCE_SYSTEM}' DETACH DELETE n").consume()
    }

    /** Each test starts from no constraints and no indexes on the benchmark labels, so what it sees is what it caused. */
    private void dropBenchSchema() {
        routing.session().run("SHOW CONSTRAINTS YIELD name, labelsOrTypes WHERE labelsOrTypes[0] STARTS WITH 'Bench' RETURN name").list().each {
            routing.session().run("DROP CONSTRAINT `${it.get('name').asString()}` IF EXISTS").consume()
        }
        routing.session().run("SHOW INDEXES YIELD name, type, labelsOrTypes WHERE type = 'RANGE' AND labelsOrTypes[0] STARTS WITH 'Bench' RETURN name").list().each {
            routing.session().run("DROP INDEX `${it.get('name').asString()}` IF EXISTS").consume()
        }
    }

    private long count(String cypher) {
        routing.session().run(cypher).single().get(0).asLong()
    }

    // ---- tests

    def "this is an Enterprise cluster of three primaries and a routing driver finds its leader"() {
        expect:
        routing.verifyConnectivity() == null
        routing.session().run("CALL dbms.components() YIELD name, edition WHERE name = 'Neo4j Kernel' RETURN edition").single().get("edition").asString() == "enterprise"
        routing.session().run("SHOW SERVERS YIELD name WHERE true RETURN count(*)").single().get(0).asLong() == 3
        PORTS.contains(leaderPort())
    }

    def "a batch written through the routing driver creates the constraint once, on the leader, and every member sees data and constraint"() {
        when:
        MessageBatchResult result = MessageBatchProcessor.process(Workloads.build("hubs3", 200, 1).messages, routing)

        then:
        result.batchSummary.errorCount == 0
        PORTS.every { int port ->
            Driver member = direct(port)
            try {
                await().atMost(30, TimeUnit.SECONDS).pollInterval(500, TimeUnit.MILLISECONDS).until {
                    member.session().run("MATCH (n:BenchNode) RETURN count(n)").single().get(0).asLong() == 200 &&
                            member.session().run("SHOW CONSTRAINTS YIELD labelsOrTypes, type WHERE labelsOrTypes = ['BenchNode'] RETURN type").list()*.get("type")*.asString() == ["NODE_PROPERTY_UNIQUENESS"]
                }
                true
            } finally {
                member.close()
            }
        }
    }

    def "a write sent to a follower is refused, and that is infrastructure, not bad data: the batch would be requeued"() {
        given:
        int follower = PORTS.find { it != leaderPort() }
        Driver followerDriver = direct(follower)

        when:
        MessageBatchProcessor.process(Workloads.build("hubs3", 20, 1).messages, followerDriver)

        then:
        Neo4jException e = thrown()
        println "write to follower on port ${follower}: ${e.class.simpleName} ${e.code()}: ${e.message}"
        !(e instanceof org.neo4j.driver.exceptions.ClientException) || !MessageBatchProcessor.isDataError(e as org.neo4j.driver.exceptions.ClientException)
        count("MATCH (n:BenchNode) RETURN count(n)") == 0

        cleanup:
        followerDriver.close()
    }

    def "KEY_CONSTRAINT=nodekey creates a NODE KEY on Enterprise"() {
        given:
        System.setProperty(PreferenceType.KEY_CONSTRAINT.name(), "nodekey")

        when:
        MessageBatchProcessor.process(Workloads.build("hubs3", 50, 2).messages, routing)

        then:
        routing.session().run("SHOW CONSTRAINTS YIELD labelsOrTypes, type WHERE labelsOrTypes = ['BenchNode'] RETURN type").list()*.get("type")*.asString() == ["NODE_KEY"]
        count("MATCH (n:BenchNode) RETURN count(n)") == 50
    }

    def "KEY_CONSTRAINT=nodekey upgrades an existing uniqueness constraint to a node key instead of silently doing nothing"() {
        given: "a node type that already has the default uniqueness constraint"
        MessageBatchProcessor.process(Workloads.build("hubs3", 20, 3).messages, routing)
        assert routing.session().run("SHOW CONSTRAINTS YIELD labelsOrTypes, type WHERE labelsOrTypes = ['BenchNode'] RETURN type").list()*.get("type")*.asString() == ["NODE_PROPERTY_UNIQUENESS"]
        SchemaManager.forgetAll()
        System.setProperty(PreferenceType.KEY_CONSTRAINT.name(), "nodekey")

        when:
        MessageBatchResult result = MessageBatchProcessor.process(Workloads.build("hubs3", 40, 3).messages, routing)

        then:
        result.batchSummary.errorCount == 0
        routing.session().run("SHOW CONSTRAINTS YIELD labelsOrTypes, type WHERE labelsOrTypes = ['BenchNode'] RETURN type").list()*.get("type")*.asString() == ["NODE_KEY"]
        count("MATCH (n:BenchNode) RETURN count(n)") == 40
    }

    def "the leader #how while 20,000 messages are being written: nothing is lost, duplicated or dead-lettered"() {
        given:
        SchemaManager.forgetAll()
        int total = 20_000
        int batchSize = 200
        List<List<String>> batches = Workloads.build("hubs3", total, 11).messages.collate(batchSize)
        Set<Integer> pending = (0..<batches.size()).toSet()
        List<Integer> processedOnce = new CopyOnWriteArrayList<>()
        List<Throwable> batchFailures = new CopyOnWriteArrayList<>()
        List<Integer> dataErrorCounts = new CopyOnWriteArrayList<>()
        int victim = leaderPort()
        long started = System.currentTimeMillis()

        when: "writers run, and once some batches are in the leader is stopped"
        Thread chaos = Thread.start {
            await().atMost(60, TimeUnit.SECONDS).until { processedOnce.size() >= 10 }
            println "stopping leader ${containerFor(victim)} (${how}) after ${processedOnce.size()} batches"
            killLeader.call(containerFor(victim))
        }
        int round = 0
        // What RabbitMQ does: a batch that was nacked is delivered again, until it has been written
        while (pending && round++ < 8) {
            List<BatchPipeline.Batch<Integer>> input = pending.sort(false).collect { int i -> new BatchPipeline.Batch<Integer>(i, batches[i]) }
            BatchPipeline.run(Flux.fromIterable(input), routing, null, 1, 2,
                    { Integer token, MessageBatchResult result ->
                        processedOnce << token
                        dataErrorCounts << result.batchSummary.errorCount
                        pending.remove(token)
                    } as BiConsumer,
                    { Integer token, Throwable failure -> batchFailures << failure } as BiConsumer, Duration.ofSeconds(1)).collectList().block(Duration.ofMinutes(4))
        }
        chaos.join(60_000)
        println "done in ${(System.currentTimeMillis() - started) / 1000}s: rounds ${round}, batches that had to be redelivered ${batchFailures.size()}: ${batchFailures*.class*.simpleName.unique()}"

        then: "every batch was eventually written, and none was ever reported as bad data"
        pending.isEmpty()
        dataErrorCounts.every { it == 0 }
        and: "the graph has each message exactly once"
        count("MATCH (n:BenchNode) RETURN count(n)") == total
        count("MATCH (n:BenchNode) RETURN count(DISTINCT n.BenchId)") == total
        ["WORKS_IN", "WORKS_ON", "MEMBER_OF"].every { String type -> count("MATCH (:BenchNode)-[r:`${type}`]->() RETURN count(r)") == total }
        ["BenchOffice", "BenchProject", "BenchTeam"].every { count("MATCH (n:`${it}`) RETURN count(n)") == 3 }

        cleanup:
        docker("start", containerFor(victim))
        awaitClusterFormed()

        where:
        how                   | killLeader
        "is stopped (SIGTERM)" | { String container -> docker("stop", "-t", "20", container) }
        "is killed (SIGKILL)"  | { String container -> docker("kill", container) }
    }
}
