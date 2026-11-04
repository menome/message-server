package com.menome.message

import groovy.transform.CompileStatic
import com.menome.util.ApplicationConfiguration
import com.menome.util.Cypher
import com.menome.util.PreferenceType

/**
 * A node to merge (or delete) in the graph. Nodes are merged on their conformed dimensions.
 *
 * All nodes that share a {@link #signature()} produce identical Cypher and can therefore be written in one UNWIND batch.
 *
 * {@link #signature()}, {@link #identity()} and {@link #dimensionKeys()} are computed on first use and cached (they are
 * asked for several times per message while batches are grouped). A node is therefore treated as immutable once it has
 * been grouped: do not change its type, dimensions or property names after calling any of them.
 */
@CompileStatic
class Neo4JNode {
    String nodeType
    String name
    String label
    Integer priority
    String sourceSystem
    Map<String, Object> conformedDimensions = [:]
    Map<String, Object> properties = [:]
    Map<String, String> dateProperties = [:]
    List<String> deleteProperties = []
    boolean deleteNode = false
    /** The raw message this node came from, kept so failures can be reported (and dead-lettered) against the original. */
    String sourceMessage
    /** Position of the source message in its batch, used to account for failures per message. */
    int sourceIndex = -1

    private List<String> cachedDimensionKeys
    private List<Object> cachedIdentity
    private String cachedSignature

    // Property names for the system-managed properties, by CASE_STYLE preference. id and created are the two the server
    // writes itself on create; ID_PROPERTY and CREATED_AT_PROPERTY override or switch them off (see PreferenceType).
    private static final Map<String, Map<String, String>> CASE_STYLES = [
            lower: [name: "name", label: "label", id: "uuid", created: "createdat", priority: "priority", sourceSystem: "sourcesystem"],
            camel: [name: "Name", label: "Label", id: "Uuid", created: "CreatedAt", priority: "Priority", sourceSystem: "SourceSystem"],
            upper: [name: "NAME", label: "LABEL", id: "UUID", created: "CREATEDAT", priority: "PRIORITY", sourceSystem: "SOURCESYSTEM"]
    ]

    private static final String OFF = "none"

    /** The last resolved names with the three settings they came from: reading them happens several times per node. */
    private static final class Resolved {
        final String style, id, created
        final Map<String, String> keys
        Resolved(String style, String id, String created, Map<String, String> keys) {
            this.style = style; this.id = id; this.created = created; this.keys = keys
        }
    }
    private static volatile Resolved resolved

    /** Property names the server manages. {@code id} and {@code created} are null when switched off. */
    static Map<String, String> keys() {
        String style = ApplicationConfiguration.getString(PreferenceType.CASE_STYLE)
        String id = ApplicationConfiguration.getString(PreferenceType.ID_PROPERTY)
        String created = ApplicationConfiguration.getString(PreferenceType.CREATED_AT_PROPERTY)
        Resolved last = resolved
        if (last != null && last.style == style && last.id == id && last.created == created) return last.keys
        Map<String, String> names = new HashMap<>(CASE_STYLES.get(style.toLowerCase(), CASE_STYLES.camel))
        if (id) names.put("id", id.equalsIgnoreCase(OFF) ? null : id)
        if (created) names.put("created", created.equalsIgnoreCase(OFF) ? null : created)
        Map<String, String> result = Collections.unmodifiableMap(names)
        resolved = new Resolved(style, id, created, result)
        result
    }

    /**
     * A message cannot set the properties the message server manages itself through Properties, DateProperties or
     * DeleteProperties: the id and created-at properties (when enabled) are generated, and Name, Label, Priority and SourceSystem come from
     * the message's own fields when it has them (a connected node has no SourceSystem field, so Properties may carry
     * it). A dimension may legitimately be called Name, but not the id or created-at property.
     *
     * Runs for every node of every message, so it looks each name up once and allocates nothing on the happy path.
     *
     * @throws IllegalArgumentException naming the first reserved property the message tries to set
     */
    void assertNoReservedProperties() {
        Map<String, String> k = keys()
        rejectReserved("Properties", properties?.keySet(), k, true)
        rejectReserved("DateProperties", dateProperties?.keySet(), k, true)
        rejectReserved("DeleteProperties", deleteProperties, k, true)
        rejectReserved("ConformedDimensions", conformedDimensions?.keySet(), k, false)
    }

    private void rejectReserved(String source, Collection<String> names, Map<String, String> k, boolean fieldBackedToo) {
        if (!names) return
        for (String candidate : names) {
            boolean reserved = candidate == k.id || candidate == k.created ||
                    (fieldBackedToo && ((name != null && candidate == k.name) || (label != null && candidate == k.label) ||
                            (priority != null && candidate == k.priority) || (sourceSystem != null && candidate == k.sourceSystem)))
            if (reserved) {
                throw new IllegalArgumentException("'${candidate}' is a reserved property name managed by the message server and cannot be set in ${source} (NodeType '${nodeType}')")
            }
        }
    }

    /** Dimension keys in a stable order so the generated Cypher is deterministic. Cached; read-only. */
    List<String> dimensionKeys() {
        if (cachedDimensionKeys == null) cachedDimensionKeys = conformedDimensions.keySet().sort(false).asImmutable()
        cachedDimensionKeys
    }

    /** Plain (non-date) property values to SET, keyed by graph property name. */
    Map<String, Object> propertyValues() {
        Map<String, String> k = keys()
        Map<String, Object> values = new TreeMap<>()
        if (name != null) values.put(k.name, name)
        if (label != null) values.put(k.label, label)
        if (priority != null) values.put(k.priority, priority)
        if (sourceSystem != null) values.put(k.sourceSystem, sourceSystem)
        values.putAll(properties ?: [:])
        dimensionKeys().each { values.remove(it) }
        values
    }

    /** Everything this node binds as a Cypher parameter. */
    Map<String, Object> parametersAsMap() {
        Map<String, Object> params = new HashMap<>(propertyValues())
        params.putAll(dateProperties ?: [:])
        params.putAll(conformedDimensions)
        params
    }

    /** Dimension values in key order. Identifies the node within a batch (last write wins). Cached. */
    List<Object> identity() {
        if (cachedIdentity == null) cachedIdentity = dimensionKeys().collect { conformedDimensions[it] }
        cachedIdentity
    }

    /**
     * Nodes with equal signatures can share one Cypher statement. Cached and interned: the same signature is the same
     * String instance, so grouping lookups hit String's cached hash code and its identity check in equals.
     */
    String signature() {
        if (cachedSignature == null) cachedSignature = computeSignature().intern()
        cachedSignature
    }

    private String computeSignature() {
        [deleteNode ? "delete" : "merge", nodeType, dimensionKeys(), propertyNames(),
         (dateProperties ?: [:]).keySet().sort(false), (deleteProperties ?: []).sort(false)].join("|")
    }

    /** The keys of {@link #propertyValues()} without building the values map. */
    private SortedSet<String> propertyNames() {
        Map<String, String> k = keys()
        SortedSet<String> names = new TreeSet<>()
        if (name != null) names.add(k.name)
        if (label != null) names.add(k.label)
        if (priority != null) names.add(k.priority)
        if (sourceSystem != null) names.add(k.sourceSystem)
        names.addAll((properties ?: [:]).keySet())
        names.removeAll(conformedDimensions.keySet())
        names
    }

    String toCypher() {
        deleteNode ? toDeleteCypher() : toMergeCypher()
    }

    String toMergeCypher() {
        Map<String, String> k = keys()
        StringBuilder cypher = new StringBuilder("MERGE (n:${Cypher.id(nodeType)} {${dimensionMatch()}})")
        List<String> onCreate = []
        if (k.id != null) onCreate << "n.${Cypher.id(k.id)} = randomUUID()".toString()
        if (k.created != null) onCreate << "n.${Cypher.id(k.created)} = datetime()".toString()
        if (onCreate) cypher << " ON CREATE SET " << onCreate.join(", ")
        List<String> sets = propertyValues().keySet().collect { String key -> ("n.${Cypher.id(key)} = ${Cypher.param(key)}").toString() }
        sets.addAll((dateProperties ?: [:]).keySet().sort(false).collect { String key -> ("n.${Cypher.id(key)} = datetime(${Cypher.param(key)})").toString() })
        if (sets) cypher << " SET " << sets.join(", ")
        if (deleteProperties) cypher << " REMOVE " << deleteProperties.sort(false).collect { String key -> ("n.${Cypher.id(key)}").toString() }.join(", ")
        cypher.toString()
    }

    String toDeleteCypher() {
        "MATCH (n:${Cypher.id(nodeType)} {${dimensionMatch()}}) DETACH DELETE n"
    }

    private String dimensionMatch() {
        dimensionKeys().collect { String key -> ("${Cypher.id(key)}: ${Cypher.param(key)}").toString() }.join(", ")
    }

    /**
     * Constraint that makes the dimension keys unique (or a node key on Enterprise), so concurrent MERGEs of the same
     * node cannot create duplicates; its backing index serves the MERGE lookup. Idempotent.
     */
    String constraintCypher(boolean nodeKey = false) {
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:${Cypher.id(nodeType)}) REQUIRE (${dimensionKeys().collect { String key -> ('n.' + Cypher.id(key)).toString() }.join(', ')}) IS ${nodeKey ? 'NODE KEY' : 'UNIQUE'}".toString()
    }

    /** Plain index that backs the MERGE lookup when no constraint can be used. Idempotent. */
    String indexCypher() {
        "CREATE INDEX IF NOT EXISTS FOR (n:${Cypher.id(nodeType)}) ON (${dimensionKeys().collect { 'n.' + Cypher.id(it) }.join(', ')})"
    }

    @Override
    boolean equals(Object o) {
        if (this.is(o)) return true
        if (getClass() != o?.getClass()) return false
        Neo4JNode node = (Neo4JNode) o
        nodeType == node.nodeType && conformedDimensions == node.conformedDimensions
    }

    @Override
    int hashCode() {
        31 * (nodeType != null ? nodeType.hashCode() : 0) + (conformedDimensions != null ? conformedDimensions.hashCode() : 0)
    }

    @Override
    String toString() {
        "Neo4JNode{nodeType='$nodeType', name='$name', priority=$priority, sourceSystem='$sourceSystem', conformedDimensions=$conformedDimensions, properties=$properties}"
    }
}
