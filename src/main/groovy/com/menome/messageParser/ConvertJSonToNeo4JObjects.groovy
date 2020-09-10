package com.menome.messageParser

import com.google.gson.Gson
import com.menome.message.Neo4JConnection
import com.menome.message.Neo4JNode

class ConvertJSonToNeo4JObjects {
    static Tuple2<List<Neo4JNode>, List<Neo4JConnection>> fromJson(String msg) {
        Map<String, Object> msgMap = new Gson().fromJson(msg, Map.class)
        def nodes = []
        def connections = []
        def primaryNode = nodeFromMap(msgMap)
        nodes << primaryNode
        msgMap.Connections.each() {
            def connectionNode = nodeFromMap(it)
            nodes << connectionNode
            Neo4JConnection connection = new Neo4JConnection(relationshipName: it.RelType, source: primaryNode, destination: connectionNode, forwardRelationship: it.ForwardRel, properties: it.Properties as Map<String, Object>)
            connections << connection
        }
        nodes
        new Tuple2(nodes, connections)
    }

    static Neo4JNode nodeFromMap(Map<String, Object> msgMap) {
        Neo4JNode node = new Neo4JNode()
        node.name = msgMap.Name
        node.nodeType = msgMap.NodeType
        node.sourceSystem = msgMap.SourceSystem
        node.priority = msgMap.Priority as Integer
        node.properties = [:]
        msgMap.Properties.each() { String k, Object v ->
            node.properties[k] = v
        }
        node.conformedDimensions = [:]
        msgMap.ConformedDimensions.each() { String k, Object v ->
            node.conformedDimensions[k] = v
        }
        node
    }

}
