package com.menome.messageBatchProcessor

import com.google.gson.Gson
import com.menome.benchmark.Workloads
import org.neo4j.driver.Driver
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionCallback
import org.neo4j.driver.TransactionContext
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.summary.ResultSummary
import spock.lang.Specification

/**
 * KANBAN E3-2: a batch writes all node statements in one transaction and all relationship statements in another.
 * Uses a fake session that models rollback (a transaction's statements only count if the whole callback succeeds),
 * so transaction counts and error isolation can be checked without a database.
 */
class PhaseTransactionSpecification extends Specification {

    /** What a fake database saw: committed statements (in order) and how many transactions were opened. */
    static class FakeDatabase {
        int transactions = 0
        List<Map> committed = []   // [cypher: String, rows: List<Map>]
        /** Decides whether the database rejects a statement. Default: the data is fine. */
        Closure<Boolean> rejects = { String cypher, List<Map> rows -> false }
        ClientException rejection = new ClientException("Neo.ClientError.Statement.TypeError", "bad value")
        ClientException outage = null

        List<Map> nodeRows(String label) {
            committed.findAll { it.cypher.contains("MERGE (n:`${label}`") }.collectMany { it.rows }
        }

        List<Map> relationshipRows(String type) {
            committed.findAll { it.cypher.contains("[r:`${type}`]") }.collectMany { it.rows }
        }
    }

    private Driver driverOver(FakeDatabase db) {
        Session session = Mock()
        session.executeWrite(_) >> { List args ->
            db.transactions++
            List<Map> buffer = []
            TransactionContext tx = Mock()
            tx.run(_, _) >> { String cypher, Map parameters ->
                if (db.outage) throw db.outage
                List<Map> rows = parameters.parms
                if (db.rejects(cypher, rows)) throw db.rejection
                buffer << [cypher: cypher, rows: rows]
                Result result = Mock()
                result.consume() >> Mock(ResultSummary)
                result
            }
            ((TransactionCallback) args[0]).execute(tx)
            db.committed.addAll(buffer)   // only reached when the whole transaction succeeded
            null
        }
        Driver driver = Mock()
        driver.session() >> session
        driver
    }

    /** Message `index` gets a Prop3 of -1, which the fake database rejects as it would an unstorable value. */
    private static List<String> withBadValueAt(List<String> messages, int index) {
        List<String> copy = new ArrayList<>(messages)
        copy[index] = copy[index].replaceFirst('"Prop3":\\d+', '"Prop3":-1')
        copy
    }

    private static final Closure<Boolean> REJECT_BAD_VALUE = { String cypher, List<Map> rows -> rows.any { it.Prop3 == -1L } }

    private static String deleteMessage(long key) {
        new Gson().toJson([Name: "gone", NodeType: Workloads.PRIMARY_TYPE, Priority: 1, SourceSystem: Workloads.SOURCE_SYSTEM,
                           ConformedDimensions: [BenchId: key], DeleteNode: true])
    }

    def "the sample message needs 2 write transactions, not 7"() {
        given:
        FakeDatabase db = new FakeDatabase()

        when:
        MessageBatchResult result = MessageBatchProcessor.process(Workloads.build("hubs3", 20, 1).messages, driverOver(db))

        then:
        result.batchSummary.errorCount == 0
        db.transactions == 2
        result.batchSummary.stages.transactions == 2
        // 4 node statements (primary + 3 hubs) in the first, 3 relationship statements in the second
        db.committed.size() == 7
    }

    def "node-only needs 1 transaction, and deletes add a third phase"() {
        given:
        FakeDatabase nodeOnly = new FakeDatabase()
        FakeDatabase withDelete = new FakeDatabase()

        when:
        MessageBatchProcessor.process(Workloads.build("node-only", 20, 1).messages, driverOver(nodeOnly))
        MessageBatchProcessor.process(Workloads.build("hubs3", 20, 1).messages + deleteMessage(999_999), driverOver(withDelete))

        then:
        nodeOnly.transactions == 1
        withDelete.transactions == 3
    }

    def "a phase with nothing in it costs no transaction"() {
        given:
        FakeDatabase db = new FakeDatabase()

        when:
        MessageBatchProcessor.process([deleteMessage(1)], driverOver(db))

        then:
        db.transactions == 1
    }

    def "statements run in order: nodes before the relationships that need them"() {
        given:
        FakeDatabase db = new FakeDatabase()

        when:
        MessageBatchProcessor.process(Workloads.build("hubs3", 5, 1).messages, driverOver(db))

        then:
        db.committed.take(4).every { it.cypher.contains("MERGE (n:") }
        db.committed.drop(4).every { it.cypher.contains("MATCH (s:") }
    }

    def "a data error in a one-statement phase is bisected down to the single bad message"() {
        given:
        FakeDatabase db = new FakeDatabase(rejects: REJECT_BAD_VALUE)
        List<String> messages = withBadValueAt(Workloads.build("node-only", 11, 1).messages, 7)

        when:
        MessageBatchResult result = MessageBatchProcessor.process(messages, driverOver(db))

        then:
        result.batchSummary.successCount == 10
        result.batchSummary.errorCount == 1
        result.errors*.errorText.every { it.contains("bad value") }
        // every good message is written exactly once; the bad one never
        db.nodeRows("BenchNode")*.BenchId.sort() == ((0..10) - 7).collect { it as long }
    }

    def "a data error among several statements rolls the phase back, then only the bad group is bisected"() {
        given: "the bad value is in the primary node group; the three hub groups are fine"
        FakeDatabase db = new FakeDatabase(rejects: REJECT_BAD_VALUE)
        List<String> messages = withBadValueAt(Workloads.build("hubs3", 12, 1).messages, 5)

        when:
        MessageBatchResult result = MessageBatchProcessor.process(messages, driverOver(db))

        then:
        result.batchSummary.successCount == 11
        result.batchSummary.errorCount == 1
        db.nodeRows("BenchNode").size() == 11
        db.nodeRows("BenchNode").every { it.BenchId != 5L }
        // The hub groups were rolled back with the failed attempt and written once by the re-run, not twice
        ["BenchOffice", "BenchProject", "BenchTeam"].every { db.nodeRows(it).size() == 3 }
        // The failed message's relationships are skipped, everyone else's are written
        ["WORKS_IN", "WORKS_ON", "MEMBER_OF"].every { String type ->
            db.relationshipRows(type).size() == 11 && db.relationshipRows(type).every { it["s:BenchId"] != 5L }
        }
    }

    def "a data error in one relationship group is isolated, and later groups skip the failed message"() {
        given: "relationship groups run in the order WORKS_IN, WORKS_ON, MEMBER_OF (by what they point at: Office, Project, Team); message 3 cannot be connected with WORKS_ON"
        FakeDatabase db = new FakeDatabase(rejects: { String cypher, List<Map> rows ->
            cypher.contains("[r:`WORKS_ON`]") && rows.any { it["s:BenchId"] == 3L }
        })

        when:
        MessageBatchResult result = MessageBatchProcessor.process(Workloads.build("hubs3", 6, 1).messages, driverOver(db))

        then:
        result.batchSummary.successCount == 5
        result.batchSummary.errorCount == 1
        // WORKS_IN ran (and committed) before message 3 was known to be bad, exactly as when each group had its own transaction
        db.relationshipRows("WORKS_IN").size() == 6
        db.relationshipRows("WORKS_ON")*.get("s:BenchId").sort() == [0L, 1L, 2L, 4L, 5L]
        // MEMBER_OF runs after the failure, so it skips message 3
        db.relationshipRows("MEMBER_OF")*.get("s:BenchId").sort() == [0L, 1L, 2L, 4L, 5L]
    }

    def "a failover (NotALeader) is not bisected: it propagates so the batch is requeued"() {
        given:
        FakeDatabase db = new FakeDatabase(outage: new ClientException("Neo.ClientError.Cluster.NotALeader", "not the leader"))

        when:
        MessageBatchProcessor.process(Workloads.build("hubs3", 20, 1).messages, driverOver(db))

        then:
        ClientException e = thrown()
        e.code() == "Neo.ClientError.Cluster.NotALeader"
        db.transactions == 1
        db.committed.isEmpty()
    }
}
