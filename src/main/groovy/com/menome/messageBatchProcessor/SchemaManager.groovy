package com.menome.messageBatchProcessor

import com.menome.message.Neo4JNode
import com.menome.util.ApplicationConfiguration
import com.menome.util.Cypher
import com.menome.util.PreferenceType
import groovy.transform.CompileStatic
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.neo4j.driver.exceptions.Neo4jException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap

/**
 * Makes sure the schema that backs the MERGE of a node type exists (KANBAN E3-1), once per node type and JVM.
 *
 * A node is merged on its dimension keys. A uniqueness constraint on them (KEY_CONSTRAINT=unique, the default) is what
 * makes concurrent MERGEs of the same node safe: without it two writers can both find nothing and both create the node.
 * The constraint's backing index serves the lookup, so there is no separate index.
 *
 * What can go wrong, and what is done about it:
 *  - an earlier version created a plain index for the same keys: Neo4j will not create the constraint next to it, so it
 *    is dropped first (the constraint brings its own);
 *  - the graph already holds duplicates: the constraint cannot be created. The plain index is put back so MERGE stays
 *    fast, the problem is logged as an error with the query that finds the duplicates, and the node type runs without the
 *    guarantee until the duplicates are fixed and the server is restarted;
 *  - KEY_CONSTRAINT=nodekey on an edition without node keys: falls back to the uniqueness constraint;
 *  - schema privileges are missing: nothing is created and it is tried again on the next batch (as before).
 * The outcome is remembered, so a failing constraint is not retried (and logged) for every batch.
 */
@CompileStatic
class SchemaManager {

    static Logger log = LoggerFactory.getLogger(SchemaManager)

    // Node types (label + dimension keys, as indexCypher() spells them) whose schema was dealt with in this JVM
    private static final Set<String> ensured = ConcurrentHashMap.newKeySet()

    // Writers run concurrently; two of them meeting a new node type at once must not both drop, create and fall back
    private static final Object DDL_LOCK = new Object()

    static void ensure(Neo4JNode node, Session session) {
        String key = node.indexCypher()
        if (ensured.contains(key)) return
        synchronized (DDL_LOCK) {
            if (ensured.contains(key)) return
            try {
                String mode = ApplicationConfiguration.getString(PreferenceType.KEY_CONSTRAINT).toLowerCase()
                if (mode == "index") {
                    run(session, node.indexCypher())
                } else {
                    ensureConstraint(node, session, mode == "nodekey")
                }
                ensured.add(key)
            } catch (Exception e) {
                // Not fatal (e.g. the user may lack schema privileges), but merges will be slow so make it visible
                log.warn("Unable to ensure the schema for {}: {} - {}", node.nodeType, key, e.message)
            }
        }
    }

    /** For tests: forget what has been created so the next batch checks the database again. */
    static void forgetAll() {
        ensured.clear()
    }

    private static void ensureConstraint(Neo4JNode node, Session session, boolean wantNodeKey) {
        String plainIndex = plainIndexName(node, session)
        if (plainIndex != null) {
            log.info("Replacing the plain index {} on {} by a constraint", plainIndex, node.nodeType)
            run(session, "DROP INDEX " + Cypher.id(plainIndex) + " IF EXISTS")
        }
        // Neo4j reports a constraint it cannot create as a database error (duplicates) or a client error (edition)
        Neo4jException failure = null
        for (boolean nodeKey : (wantNodeKey ? [true, false] : [false])) {
            try {
                run(session, node.constraintCypher(nodeKey))
                return
            } catch (Neo4jException e) {
                failure = e
                if (nodeKey) log.warn("A node key constraint cannot be created for {} ({}), using a uniqueness constraint", node.nodeType, e.message)
            }
        }
        // No constraint possible. Keep MERGE index-backed, and make the missing guarantee impossible to overlook.
        run(session, node.indexCypher())
        log.error("Could not create a uniqueness constraint for {} on {}: {}. A plain index is used instead, so MERGE is NOT protected " +
                "against duplicate nodes when several writers run. Find the duplicates with: MATCH (n:{}) WITH {} AS key, count(*) AS c " +
                "WHERE c > 1 RETURN key, c; merge or delete them, then restart the message server.",
                node.nodeType, node.dimensionKeys(), failure?.message, Cypher.id(node.nodeType),
                node.dimensionKeys().collect { String k -> ('n.' + Cypher.id(k)).toString() }.join(', '))
    }

    /** The name of a plain (not constraint-owned) range index on exactly this label and these keys, if there is one. */
    private static String plainIndexName(Neo4JNode node, Session session) {
        Result result = session.run(Cypher.versioned(
                "SHOW INDEXES YIELD name, type, labelsOrTypes, properties, owningConstraint " +
                        "WHERE type = 'RANGE' AND labelsOrTypes = [\$label] AND properties = \$keys AND owningConstraint IS NULL RETURN name"),
                [label: node.nodeType, keys: node.dimensionKeys()] as Map<String, Object>)
        result.hasNext() ? result.next().get("name").asString() : null
    }

    private static void run(Session session, String ddl) {
        session.run(Cypher.versioned(ddl)).consume()
    }
}
