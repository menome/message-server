package com.menome.messageParser

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.ToNumberPolicy
import com.menome.message.Neo4JConnection
import com.menome.message.Neo4JNode
import groovy.transform.CompileStatic

/** Runs for every message: statically compiled and free of closures so the cost is the parsing itself. */
@CompileStatic
class ConvertJSonToNeo4JObjects {

    // Whole numbers stay Long (not Double) so integer dimensions such as EmployeeId match existing integer-keyed nodes.
    private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create()

    static Tuple2<List<Neo4JNode>, List<Neo4JConnection>> fromJson(String msg, int index = -1) {
        Map<String, Object> msgMap = (Map<String, Object>) GSON.fromJson(msg, Map)
        List<Neo4JNode> nodes = new ArrayList<>()
        List<Neo4JConnection> connections = new ArrayList<>()

        Neo4JNode primaryNode = nodeFromMap(msgMap, msg, index)
        nodes.add(primaryNode)
        // Connections of a node that is being deleted are moot
        if (!primaryNode.deleteNode) {
            List<Map<String, Object>> connectionMaps = (List<Map<String, Object>>) msgMap.get("Connections")
            if (connectionMaps != null) {
                for (Map<String, Object> connectionMap : connectionMaps) {
                    Neo4JNode connectionNode = nodeFromMap(connectionMap, msg, index)
                    if (connectionMap.get("DeleteRelationship")) {
                        // Only the relationship goes, not the node it points at
                        connections.add(connectionFromMap(connectionMap, primaryNode, connectionNode, msg, index))
                    } else {
                        nodes.add(connectionNode)
                        if (!connectionNode.deleteNode) {
                            connections.add(connectionFromMap(connectionMap, primaryNode, connectionNode, msg, index))
                        }
                    }
                }
            }
        }
        new Tuple2<List<Neo4JNode>, List<Neo4JConnection>>(nodes, connections)
    }

    static Neo4JConnection connectionFromMap(Map<String, Object> it, Neo4JNode source, Neo4JNode destination, String msg, int index) {
        new Neo4JConnection(
                relationshipName: (String) it.get("RelType"),
                source: source,
                destination: destination,
                forwardRelationship: it.get("ForwardRel") as boolean,
                properties: mapOf(it, "RelProps"),
                dateProperties: stringMapOf(it, "DateRelProps"),
                deleteProperties: listOf(it, "DeleteRelProps"),
                deleteRelationship: it.get("DeleteRelationship") as boolean,
                sourceMessage: msg,
                sourceIndex: index)
    }

    static Neo4JNode nodeFromMap(Map<String, Object> msgMap, String msg = null, int index = -1) {
        Object priority = msgMap.get("Priority")
        Neo4JNode node = new Neo4JNode(
                name: (String) msgMap.get("Name"),
                nodeType: (String) msgMap.get("NodeType"),
                label: (String) msgMap.get("Label"),
                sourceSystem: (String) msgMap.get("SourceSystem"),
                priority: priority != null ? ((Number) priority).intValue() : (Integer) null,
                properties: mapOf(msgMap, "Properties"),
                dateProperties: stringMapOf(msgMap, "DateProperties"),
                conformedDimensions: mapOf(msgMap, "ConformedDimensions"),
                deleteProperties: listOf(msgMap, "DeleteProperties"),
                deleteNode: msgMap.get("DeleteNode") as boolean,
                sourceMessage: msg,
                sourceIndex: index)
        node.assertNoReservedProperties()
        node
    }

    /** A private, mutable copy of the object under `key`, empty when the message does not have it. */
    private static Map<String, Object> mapOf(Map<String, Object> from, String key) {
        Map<String, Object> value = (Map<String, Object>) from.get(key)
        value == null ? new LinkedHashMap<String, Object>() : new LinkedHashMap<String, Object>(value)
    }

    private static Map<String, String> stringMapOf(Map<String, Object> from, String key) {
        Map<String, String> value = (Map<String, String>) from.get(key)
        value == null ? new LinkedHashMap<String, String>() : new LinkedHashMap<String, String>(value)
    }

    private static List<String> listOf(Map<String, Object> from, String key) {
        List<String> value = (List<String>) from.get(key)
        value == null ? new ArrayList<String>() : value
    }
}
