package com.menome.messageBatchProcessor

import com.menome.message.Neo4JConnection
import com.menome.message.Neo4JNode

class MessageBatchCollection {

    String nodeType
    HashSet<Neo4JNode> nodes = []
    HashSet<Neo4JConnection> connections = []

    MessageBatchCollection(String nodeType) {
        this.nodeType = nodeType
    }

    Neo4JNode prototypeNode() {
        nodes.first()
    }

    def addNode(Neo4JNode node) {
        if (node.nodeType != nodeType) {
            throw new RuntimeException("Cannot add node of type ${node.nodeType} ${node.conformedDimensionNeo4JSyntax()} to collection of $nodeType")
        }
        nodes.addAll(node)
    }

    def addConnections(List<Neo4JConnection> neo4JConnections) {
        if (neo4JConnections) {
            connections.addAll(neo4JConnections)
        }
    }

    def nodesAsParameterList() {
        List parms = nodes.collect() { Neo4JNode node ->
            node.parametersAsMap()
        }
        ["parms": parms]
    }

    static List<MessageBatchCollection> split(MessageBatchCollection batch){
        HashSet<Neo4JNode> nodes = batch.nodes
        List<Neo4JNode> segments = nodes.collate(nodes.size().intdiv(2), true)

        List<MessageBatchCollection> splitCollection = []
        MessageBatchCollection part1 = new MessageBatchCollection(batch.nodeType)
        part1.nodes = segments[0]
        splitCollection.add(part1)

        MessageBatchCollection part2 = new MessageBatchCollection(batch.nodeType)
        part2.nodes = segments[1]
        splitCollection.add(part2)

        splitCollection
    }

    @Override
    String toString() {
        String output = "NodeType:${nodeType}:${nodes.size()}\n"
        nodes.each { Neo4JNode node ->
            output += node.toString() + "\n"
        }
        output
    }
}
