package com.menome.messageBatchProcessor

import com.google.gson.Gson
import com.menome.message.Neo4JConnection
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageConnectionCollection {
    String relationshipName
    HashSet<Neo4JConnection> connections = []

    static Logger log = LoggerFactory.getLogger(MessageConnectionCollection.class)

    MessageConnectionCollection(String relationshipName) {
        this.relationshipName = relationshipName
    }

    def addConnection(Neo4JConnection connection) {
        if (connection) {
            connections.add(connection)
        }
    }

    def relationshipsAsParameterList() {
        List parms = connections.collect() { Neo4JConnection connection ->
            def connectionParms = [:]
            connectionParms.putAll(connection.source.conformedDimensions.collectEntries {["source$it.key".toString(),it.value]})
            connectionParms.putAll(connection.destination.conformedDimensions.collectEntries {["destination$it.key".toString(),it.value]})
            connectionParms.put("Properties",connection.properties?:[:])
            connectionParms
        }
        if (log.isDebugEnabled()){
            log.debug(new Gson().toJson(parms))
        }

        ["parms": parms]
    }

    Neo4JConnection prototypeRelationship() {
        connections.first()
    }

    /*
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

     */

    static List<MessageConnectionCollection> split(MessageConnectionCollection batch){
        HashSet<Neo4JConnection> connections = batch.connections
        List<Neo4JConnection> segments = connections.collate(connections.size().intdiv(2), true)

        List<Neo4JConnection> splitCollection = []
        MessageConnectionCollection part1 = new MessageConnectionCollection(batch.relationshipName)
        part1.connections = segments[0]
        splitCollection.add(part1)

        MessageConnectionCollection part2 = new MessageConnectionCollection(batch.relationshipName)
        part2.connections = segments[1]
        splitCollection.add(part2)

        splitCollection
    }



    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        MessageConnectionCollection that = (MessageConnectionCollection) o

        if (relationshipName != that.relationshipName) return false

        return true
    }

    int hashCode() {
        return (relationshipName != null ? relationshipName.hashCode() : 0)
    }
}
