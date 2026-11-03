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

/**
 * KANBAN E3-1 / E3-12: which schema statements run, in which order, and what happens when the constraint cannot be created.
 * The fake models what Neo4j 2026.10 really does (checked against Community and Enterprise): a constraint of the same kind
 * is a no-op under IF NOT EXISTS, a node key next to a uniqueness constraint (or the reverse) is "conflicting constraint
 * already exists", and a plain index next to a constraint on the same keys cannot exist.
 */
class SchemaManagerSpecification extends Specification {

    static final String UNIQUE = "NODE_PROPERTY_UNIQUENESS"
    static final String NODE_KEY = "NODE_KEY"

    static class FakeSchema {
        List<String> ddl = []                         // every DDL statement sent, in order
        int showQueries = 0
        String plainIndex = null                      // name of a pre-existing plain index, if any
        Map<String, String> constraints = [:]         // type -> name of constraints on the default node (Person on Email)
        Map<String, Map<String, String>> others = [:] // the same for any other label + keys, which are separate schemas
        String edition = "community"
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

    private static Result resultOf(List<String> values) {
        Iterator<String> remaining = values.iterator()
        [hasNext: { remaining.hasNext() },
         next   : {
             String value = remaining.next()
             [get: { String column -> [asString: { value }] as Value }] as Record
         },
         consume: { null }] as Result
    }

    private static Neo4jException conflict() {
        new ClientException("Neo.ClientError.Schema.ConstraintCreationFailed", "conflicting constraint already exists")
    }

    /** Constraints of one label + keys. The default node (Person on Email) uses schema.constraints, as the tests seed it. */
    private static Map<String, String> constraintsOn(FakeSchema schema, String label, List<String> keys) {
        "${label}|${keys}".toString() == "Person|[Email]" ? schema.constraints : schema.others.computeIfAbsent("${label}|${keys}".toString(), { [:] })
    }

    private static Session sessionOver(FakeSchema schema) {
        [run: { Object... args ->
            String statement = args[0]
            if (args.length == 2) {   // SHOW queries carry parameters
                schema.showQueries++
                if (statement.contains("SHOW CONSTRAINTS")) {
                    Map<String, String> existing = constraintsOn(schema, args[1].label, args[1].keys)
                    if (statement.contains("RETURN type")) return resultOf(existing.keySet().toList())
                    String type = (statement =~ /type = '(\w+)'/)[0][1]
                    return resultOf(existing.containsKey(type) ? [existing[type]] : [])
                }
                return resultOf(schema.plainIndex != null ? [schema.plainIndex] : [])
            }
            if (statement.contains("dbms.components")) return resultOf([schema.edition])
            schema.ddl << statement
            if (schema.failing(statement)) throw schema.failure()
            if (statement.contains("DROP CONSTRAINT")) {
                String name = (statement =~ /DROP CONSTRAINT `([^`]+)`/)[0][1]
                ([schema.constraints] + schema.others.values().toList()).each { Map<String, String> m -> m.values().removeAll { it == name } }
            } else if (statement.contains("CREATE CONSTRAINT")) {
                String label = (statement =~ /FOR \(n:`([^`]+)`\)/)[0][1]
                List<String> keys = (statement =~ /n\.`([^`]+)`/).collect { it[1] }
                Map<String, String> existing = constraintsOn(schema, label, keys)
                if (statement.contains("IS NODE KEY")) {
                    if (existing.containsKey(UNIQUE)) throw conflict()
                    existing.put(NODE_KEY, "created_node_key")
                } else {
                    if (existing.containsKey(NODE_KEY)) throw conflict()
                    existing.putIfAbsent(UNIQUE, "created_unique")
                }
            }
            resultOf([])
        }] as Session
    }

    private static Neo4JNode node(String type = "Person", Map dimensions = [Email: "a@b.c"]) {
        new Neo4JNode(nodeType: type, conformedDimensions: dimensions)
    }

    private static void useMode(String mode) {
        System.setProperty(PreferenceType.KEY_CONSTRAINT.name(), mode)
    }

    /** "CREATE CONSTRAINT" / "CREATE CONSTRAINT (node key)" / "DROP INDEX" ... one short label per statement. */
    private static List<String> shape(FakeSchema schema) {
        schema.ddl.collect { String statement ->
            statement.replace("CYPHER 25 ", "").split(" ").take(2).join(" ") + (statement.contains("NODE KEY") ? " (node key)" : "")
        }
    }

    def "the default is one uniqueness constraint on the dimension keys and no separate index"() {
        given:
        FakeSchema schema = new FakeSchema()

        when:
        SchemaManager.ensure(node("Person", [Id: 1, Email: "x"]), sessionOver(schema))

        then:
        schema.ddl == ['CYPHER 25 CREATE CONSTRAINT IF NOT EXISTS FOR (n:`Person`) REQUIRE (n.`Email`, n.`Id`) IS UNIQUE']
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
        shape(schema) == ["DROP INDEX", "CREATE CONSTRAINT", "CREATE INDEX"]
        schema.ddl.size() == afterFirst
    }

    def "node key is tried first when asked for, and falls back to a uniqueness constraint where it is not supported"() {
        given:
        useMode("nodekey")
        // Community says "not supported in this edition" as a client error
        FakeSchema schema = new FakeSchema(failing: { it.contains("IS NODE KEY") },
                failure: { new ClientException("Neo.ClientError.Statement.NotSupported", "Key constraint is not supported in community edition") })

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        shape(schema) == ["CREATE CONSTRAINT (node key)", "CREATE CONSTRAINT"]
        schema.constraints == [(UNIQUE): "created_unique"]
    }

    def "node key is used when the edition supports it"() {
        given:
        useMode("nodekey")
        FakeSchema schema = new FakeSchema(edition: "enterprise")

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        shape(schema) == ["CREATE CONSTRAINT (node key)"]
        schema.constraints == [(NODE_KEY): "created_node_key"]
    }

    def "a constraint that already exists is enough: after a restart nothing is issued, in any mode"() {
        given:
        useMode(mode)
        FakeSchema schema = new FakeSchema(constraints: existing, edition: "enterprise")

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        schema.ddl.isEmpty()

        where:
        mode      | existing
        "unique"  | [(UNIQUE): "c1"]
        "nodekey" | [(NODE_KEY): "c2"]
        "unique"  | [(NODE_KEY): "c3"]      // a node key already guarantees uniqueness: switching back must not fail
    }

    def "nodekey where a uniqueness constraint exists, on Enterprise: the uniqueness constraint is swapped for the node key"() {
        given:
        useMode("nodekey")
        FakeSchema schema = new FakeSchema(constraints: [(UNIQUE): "constraint_old"], edition: "enterprise")

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        shape(schema) == ["DROP CONSTRAINT", "CREATE CONSTRAINT (node key)"]
        schema.constraints == [(NODE_KEY): "created_node_key"]
    }

    def "if the node key cannot replace the uniqueness constraint, the uniqueness constraint is put back"() {
        given:
        useMode("nodekey")
        FakeSchema schema = new FakeSchema(constraints: [(UNIQUE): "constraint_old"], edition: "enterprise", failing: { it.contains("IS NODE KEY") })

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        noExceptionThrown()
        shape(schema) == ["DROP CONSTRAINT", "CREATE CONSTRAINT (node key)", "CREATE CONSTRAINT"]
        schema.constraints.containsKey(UNIQUE)
    }

    def "nodekey on an edition without node keys never touches the uniqueness constraint it has"() {
        given:
        useMode("nodekey")
        FakeSchema schema = new FakeSchema(constraints: [(UNIQUE): "constraint_old"], edition: "community")

        when:
        SchemaManager.ensure(node(), sessionOver(schema))

        then:
        schema.ddl.isEmpty()
        schema.constraints == [(UNIQUE): "constraint_old"]
    }

    def "KEY_CONSTRAINT=index keeps the old behaviour: a plain index and nothing else"() {
        given:
        useMode("index")
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
