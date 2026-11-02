package com.menome.messageBatchProcessor

import com.google.gson.Gson
import com.menome.benchmark.Workloads
import com.menome.message.Neo4JNode
import org.neo4j.driver.Driver
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionCallback
import org.neo4j.driver.TransactionContext
import spock.lang.Specification

import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * KANBAN E4-2 / E3-7: writers working on overlapping nodes at the same time must take their locks in the same order, or
 * they deadlock. Statements and rows therefore go out in an order that depends only on the data, never on message order.
 */
class LockOrderingSpecification extends Specification {

    /** Records every statement that was run in a transaction that completed, in order. */
    static class Recorder {
        final List<Map> committed = []

        Driver driver() {
            Result result = [consume: { null }] as Result
            Session session = [executeWrite: { Object... args ->
                List<Map> buffer = []
                TransactionContext tx = [run: { Object... run -> buffer << [cypher: run[0], rows: ((Map) run[1]).parms]; result }] as TransactionContext
                ((TransactionCallback) args[0]).execute(tx)
                committed.addAll(buffer)
                null
            }, close: { }] as Session
            [session: { Object... args -> session }, close: { }] as Driver
        }
    }

    def "values of one type compare naturally and mixed types still have one fixed order"() {
        expect:
        LockOrder.compare([1L, "b"], [1L, "c"]) < 0
        LockOrder.compare([2L], [10L]) < 0
        LockOrder.compare(["a"], ["a"]) == 0
        LockOrder.compare([1L], [1L, 5L]) < 0
        // Long vs String has no natural order; it must still be the same answer both ways round, and never "equal"
        LockOrder.compare([1L], ["1"]) == -LockOrder.compare(["1"], [1L])
        LockOrder.compare([1L], ["1"]) != 0
        LockOrder.compare([null], [1L]) < 0
    }

    def "sorting by it gives the same order whatever order the input came in"() {
        given:
        List<List<Object>> keys = [[3L, "x"], [1L, "z"], ["text"], [1L, "a"], [null], [2L]]

        expect:
        (1..20).every { seed ->
            List<List<Object>> shuffled = new ArrayList<>(keys)
            Collections.shuffle(shuffled, new Random(seed))
            shuffled.sort { a, b -> LockOrder.compare(a, b) } == keys.sort(false) { a, b -> LockOrder.compare(a, b) }
        }
    }

    def "the same messages in any order produce the same statements, in the same order, with the same rows in the same order"() {
        given:
        List<String> messages = Workloads.build("hubs3", 60, 1).messages

        when:
        List<List<Map>> outcomes = (1..5).collect { int seed ->
            List<String> shuffled = new ArrayList<>(messages)
            Collections.shuffle(shuffled, new Random(seed))
            Recorder recorder = new Recorder()
            MessageBatchProcessor.process(shuffled, recorder.driver())
            recorder.committed
        }

        then:
        outcomes.every { it == outcomes[0] }
        // and it is a real sequence: 4 node statements, then 3 relationship statements
        outcomes[0].size() == 7
    }

    def "statements are ordered by label, relationships by the label they point at, merges before deletes"() {
        given:
        Recorder recorder = new Recorder()
        List<String> messages = [
                Workloads.build("hubs3", 5, 1).messages,
                [new Gson().toJson([Name: "x", NodeType: "BenchNode", Priority: 1, SourceSystem: Workloads.SOURCE_SYSTEM, ConformedDimensions: [BenchId: 99999L], DeleteNode: true])]
        ].flatten() as List<String>

        when:
        MessageBatchProcessor.process(messages, recorder.driver())
        List<String> cyphers = recorder.committed*.cypher.collect { it.replace("CYPHER 25 UNWIND \$parms AS param ", "") }

        then: "BenchNode < BenchOffice < BenchProject < BenchTeam, then the relationships in the same label order of what they point at, then the delete"
        cyphers.take(4).collect { (it =~ /MERGE \(n:`(\w+)`/)[0][1] } == ["BenchNode", "BenchOffice", "BenchProject", "BenchTeam"]
        cyphers.drop(4).take(3).collect { (it =~ /\[r:`(\w+)`\]/)[0][1] } == ["WORKS_IN" /* Office */, "WORKS_ON" /* Project */, "MEMBER_OF" /* Team */]
        cyphers.last().contains("DETACH DELETE")
    }

    def "a node that appears under two property sets keeps the last message's value, as before"() {
        given: "message 1 sets A=3; message 2 (later) has a different property set and sets A=1"
        Recorder recorder = new Recorder()
        String first = new Gson().toJson([Name: "n", NodeType: "T", Priority: 1, SourceSystem: "s", ConformedDimensions: [Id: 1L], Properties: [A: 3L]])
        String second = new Gson().toJson([Name: "n", NodeType: "T", Priority: 1, SourceSystem: "s", ConformedDimensions: [Id: 1L], Properties: [A: 1L, B: 2L]])

        when:
        MessageBatchProcessor.process([first, second], recorder.driver())
        List<Map> statements = recorder.committed

        then: "the two groups run in message order, so the later message's A=1 is applied last"
        statements.size() == 2
        statements[0].rows[0].A == 3L
        statements[1].rows[0].A == 1L
    }

    def "schema DDL for a node type is issued once even when several writers meet it at the same time"() {
        given:
        SchemaManager.forgetAll()
        AtomicInteger constraints = new AtomicInteger()
        Result result = [consume: { null }, hasNext: { false }] as Result
        Session session = [run: { Object... args ->
            if (args[0].toString().contains("CREATE CONSTRAINT")) {
                constraints.incrementAndGet()
                Thread.sleep(100)   // long enough for the others to arrive while this one is busy
            }
            result
        }] as Session
        int writers = 8
        CyclicBarrier together = new CyclicBarrier(writers)
        def pool = Executors.newFixedThreadPool(writers)

        when:
        (1..writers).collect { pool.submit({ together.await(); SchemaManager.ensure(new Neo4JNode(nodeType: "LockTestNode", conformedDimensions: [Id: 1]), session) } as Runnable) }*.get(20, TimeUnit.SECONDS)

        then:
        constraints.get() == 1

        cleanup:
        pool.shutdown()
        SchemaManager.forgetAll()
    }
}
