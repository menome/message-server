package com.menome.messageBatchProcessor

import com.menome.message.Neo4JNode
import com.menome.util.PreferenceType
import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.Value
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.exceptions.DatabaseException
import org.neo4j.driver.exceptions.Neo4jException
import spock.lang.Specification

/** KANBAN E3-1: which schema statements run, in which order, and what happens when the constraint cannot be created. */
class SchemaManagerSpecification extends Specification {

    static class FakeSchema {
        List<String> ddl = []                         // every DDL statement sent, in order
        int showQueries = 0
        String plainIndex = null                      // name of a pre-existing plain index, if any
        Closure<Boolean> failing = { String statement -> false }
        // What Neo4j really throws when a constraint cannot be created because the graph holds duplicates (a database
        // error, not a client error; a fake that threw ClientException hid a bug here)
        Closure<Neo4jException> failure = { new DatabaseException("Neo.DatabaseError.Schema.ConstraintCreationFailed", "Unable to create constraint: duplicates") }

        List<String> matching(String text) { ddl.findAll { it.contains(text) } }
    }

    def setup() {
        SchemaManager.forgetAll()
    }

    def cleanup() {
        SchemaManager.forgetAll()
        System.clearProperty(PreferenceType.KEY_CONSTRAINT.name())
    }

    private Session sessionOver(FakeSchema schema) {
        Session session = Mock()
        session.run(_ as String, _ as Map) >> {
            schema.showQueries++
            Result result = Mock()
            result.hasNext() >> (schema.plainIndex != null)
            Value name = Mock()
            name.asString() >> schema.plainIndex
            Record record = Mock()
            record.get("name") >> name
            result.next() >> record
            result
        }
        session.run(_ as String) >> { String statement ->
            schema.ddl << statement
            if (schema.failing(statement)) throw schema.failure()
            Mock(Result)
        }
        session
    }

    private static Neo4JNode node(String type = "Person", Map dimensions = [Email: "a@b.c"]) {
        new Neo4JNode(nodeType: type, conformedDimensions: dimensions)
    }

    def "the default is one uniqueness constraint on the dimension keys and no separate index"() {
        given:
        FakeSchema schema = new FakeSchema()

        when:
        SchemaManager.ensure(node("Person", [Id: 1, Email: "x"]), sessionOver(schema))

        then:
        schema.ddl.size() == 1
        schema.ddl[0] == 'CYPHER 25 CREATE CONSTRAINT IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`Email`, n.`Id`) IS UNIQUE'
    }

    def "a node type is dealt with once per JVM: the next batch sends nothing"() {
        given:
        FakeSchema schema = new FakeSchema()
        Session session = sessionOver(schema)

        when:
        SchemaManager.ensure(node(), session)
        int afterFirst = schema.ddl.size() + schema.showQueries
        SchemaManager.ensure(node(), session)
        SchemaManager.ensure(node("Person", [Email: "different value, same keys"]), session)

        then:
        schema.ddl.size() + schema.showQueries == afterFirst
    }

    def "different dimension keys on the same label are a different schema"() {
        given:
        FakeSchema schema = new FakeSchema()
        Session session = sessionOver(schema)

        when:
        SchemaManager.ensure(node("Person", [Email: "x"]), session)
        SchemaManager.ensure(node("Person", [Id: 1]), session)

        then:
        schema.matching("CREATE CONSTRAINT").size() == 2
    }

    def "a plain index left by an earlier version is dropped before the constraint is created"() {
        given:
        FakeSchema schema = new FakeSchema(plainIndex: "index_5f3a")

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        schema.ddl.size() == 2
        schema.ddl[0] == 'CYPHER 25 DROP INDEX `index_5f3a` IF EXISTS'
        schema.ddl[1].contains("CREATE CONSTRAINT")
    }

    def "when the graph already has duplicates the plain index is restored, the node type works, and the failure is not retried per batch"() {
        given:
        FakeSchema schema = new FakeSchema(plainIndex: "index_5f3a", failing: { it.contains("CREATE CONSTRAINT") })
        Session session = sessionOver(schema)

        when:
        SchemaManager.ensure(node(), session)
        int afterFirst = schema.ddl.size()
        SchemaManager.ensure(node(), session)
        SchemaManager.ensure(node(), session)

        then:
        noExceptionThrown()
        schema.ddl*.replace("CYPHER 25 ", "").collect { it.split(" ").take(2).join(" ") } == ["DROP INDEX", "CREATE CONSTRAINT", "CREATE INDEX"]
        schema.matching("CREATE INDEX").size() == 1
        schema.ddl.size() == afterFirst
    }

    def "node key is tried first when asked for, and falls back to a uniqueness constraint where it is not supported"() {
        given:
        System.setProperty(PreferenceType.KEY_CONSTRAINT.name(), "nodekey")
        // Community says "not supported in this edition" as a client error
        FakeSchema schema = new FakeSchema(failing: { it.contains("IS NODE KEY") },
                failure: { new ClientException("Neo.ClientError.Statement.NotSupported", "Key constraint is not supported in community edition") })

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        schema.ddl.size() == 2
        schema.ddl[0].endsWith("IS NODE KEY")
        schema.ddl[1].endsWith("IS UNIQUE")
    }

    def "node key is used when the edition supports it"() {
        given:
        System.setProperty(PreferenceType.KEY_CONSTRAINT.name(), "nodekey")
        FakeSchema schema = new FakeSchema()

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        schema.ddl.size() == 1
        schema.ddl[0].endsWith("IS NODE KEY")
    }

    def "KEY_CONSTRAINT=index keeps the old behaviour: a plain index and nothing else"() {
        given:
        System.setProperty(PreferenceType.KEY_CONSTRAINT.name(), "index")
        FakeSchema schema = new FakeSchema(plainIndex: "index_5f3a")

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        schema.ddl.size() == 1
        schema.ddl[0].contains("CREATE INDEX IF NOT EXISTS")
        schema.showQueries == 0
    }

    def "when even the fallback fails (no schema privileges) nothing is remembered and the next batch tries again"() {
        given:
        FakeSchema schema = new FakeSchema(failing: { true })
        Session session = sessionOver(schema)

        when:
        SchemaManager.ensure(node(), session)
        int afterFirst = schema.ddl.size()
        SchemaManager.ensure(node(), session)

        then:
        noExceptionThrown()
        schema.ddl.size() > afterFirst
    }

    def "the constraint statement is generated for a node key and for a single key"() {
        expect:
        node("Person", [Email: "x"]).constraintCypher() == 'CREATE CONSTRAINT IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`Email`) IS UNIQUE'
        node("Person", [Email: "x"]).constraintCypher(true) == 'CREATE CONSTRAINT IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`Email`) IS NODE KEY'
        node("We`ird", [Email: "x"]).constraintCypher().contains('(n:`We``ird`)')
    }
}
