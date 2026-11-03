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
 *  - a constraint on the same keys already exists: a node key satisfies both modes and a uniqueness constraint satisfies
 *    "unique", so nothing is created (asking for the other kind would fail with "conflicting constraint already exists",
 *    which is why this is checked first);
 *  - KEY_CONSTRAINT=nodekey and a uniqueness constraint exists: on Enterprise it is swapped for the node key (and restored
 *    if that fails); on an edition without node keys it is left alone, so no constraint is dropped and rebuilt for nothing;
 *  - KEY_CONSTRAINT=nodekey on an edition without node keys and no constraint yet: a uniqueness constraint is created;
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
        enterprise = null
    }

    private static void ensureConstraint(Neo4JNode node, Session session, boolean wantNodeKey) {
        String plainIndex = plainIndexName(node, session)
        if (plainIndex != null) {
            log.info("Replacing the plain index {} on {} by a constraint", plainIndex, node.nodeType)
            run(session, "DROP INDEX " + Cypher.id(plainIndex) + " IF EXISTS")
        }
        Set<String> existing = constraintTypes(session, node)
        if (existing.contains(NODE_KEY) || (!wantNodeKey && existing.contains(UNIQUENESS))) return   // already guaranteed
        if (wantNodeKey && existing.contains(UNIQUENESS)) {
            // A node key cannot be created next to the uniqueness constraint, so swap them, but only where node keys exist
            if (isEnterprise(session)) upgradeUniquenessToNodeKey(node, session)
            return
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

    private static final String UNIQUENESS = "NODE_PROPERTY_UNIQUENESS"
    private static final String NODE_KEY = "NODE_KEY"

    /** Swap the uniqueness constraint for a node key; if the node key cannot be created, put the uniqueness constraint back. */
    private static void upgradeUniquenessToNodeKey(Neo4JNode node, Session session) {
        String uniqueness = constraintName(session, node, UNIQUENESS)
        if (uniqueness == null) return
        log.info("Replacing the uniqueness constraint {} on {} by a node key", uniqueness, node.nodeType)
        run(session, "DROP CONSTRAINT " + Cypher.id(uniqueness) + " IF EXISTS")
        try {
            run(session, node.constraintCypher(true))
        } catch (Neo4jException e) {
            log.warn("The node key for {} could not replace the uniqueness constraint ({}), restoring it", node.nodeType, e.message)
            run(session, node.constraintCypher(false))
        }
    }

    /** The types of the constraints that exist on exactly this label and these keys. */
    private static Set<String> constraintTypes(Session session, Neo4JNode node) {
        Result result = session.run(Cypher.versioned(
                "SHOW CONSTRAINTS YIELD type, labelsOrTypes, properties WHERE labelsOrTypes = [\$label] AND properties = \$keys RETURN type"),
                [label: node.nodeType, keys: node.dimensionKeys()] as Map<String, Object>)
        Set<String> types = new HashSet<>()
        while (result.hasNext()) types.add(result.next().get("type").asString())
        types
    }

    /** The name of the constraint of this type on exactly this label and these keys, if there is one. */
    private static String constraintName(Session session, Neo4JNode node, String type) {
        Result result = session.run(Cypher.versioned(
                "SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties WHERE type = '" + type + "' AND labelsOrTypes = [\$label] AND properties = \$keys RETURN name"),
                [label: node.nodeType, keys: node.dimensionKeys()] as Map<String, Object>)
        result.hasNext() ? result.next().get("name").asString() : null
    }

    private static volatile Boolean enterprise

    /** Node keys exist only on Enterprise. Asked of the server once. */
    private static boolean isEnterprise(Session session) {
        if (enterprise == null) {
            Result result = session.run(Cypher.versioned("CALL dbms.components() YIELD name, edition WHERE name = 'Neo4j Kernel' RETURN edition"))
            enterprise = result.hasNext() && result.next().get("edition").asString().equalsIgnoreCase("enterprise")
        }
        enterprise
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
