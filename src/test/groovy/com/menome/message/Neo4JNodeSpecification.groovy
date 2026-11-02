package com.menome.message

import com.menome.messageBatchProcessor.MessageBatchCollection
import com.menome.messageParser.ConvertJSonToNeo4JObjects
import com.menome.util.Cypher
import com.menome.util.PreferenceType
import spock.lang.Specification
import spock.lang.Unroll

class Neo4JNodeSpecification extends Specification {

    def cleanup() {
        System.clearProperty(PreferenceType.CASE_STYLE.name())
    }

    @Unroll
    def "splitting #size items loses nothing"() {
        given:
        def items = (1..size).toList()
        when:
        def halves = MessageBatchCollection.split(items)
        then:
        halves.size() == 2
        (halves[0] + halves[1]) == items
        halves[0].size() == size.intdiv(2)
        where:
        size << [2, 3, 5, 7, 9, 10, 501]
    }

    def "case style drives the system property names"() {
        given:
        System.setProperty(PreferenceType.CASE_STYLE.name(), style)
        def node = new Neo4JNode(nodeType: "Person", name: "Bob", conformedDimensions: [Id: 1])
        expect:
        node.toCypher().contains("n.`$uuid` = randomUUID()")
        node.propertyValues().keySet() == [name] as Set
        where:
        style   | uuid   | name
        "camel" | "Uuid" | "Name"
        "lower" | "uuid" | "name"
        "upper" | "UUID" | "NAME"
        "bogus" | "Uuid" | "Name"
    }

    def "identifiers are quoted so they cannot break out of the statement"() {
        expect:
        Cypher.id("a`b") == "`a``b`"
        def node = new Neo4JNode(nodeType: "Person", conformedDimensions: [Id: 1], properties: ["x` = 1 DETACH DELETE n //": "v"])
        node.toCypher().contains("n.`x`` = 1 DETACH DELETE n //` = param.`x`` = 1 DETACH DELETE n //`")
    }

    def "nodes with different property keys have different signatures, same keys share one"() {
        given:
        def a = new Neo4JNode(nodeType: "Person", conformedDimensions: [Id: 1], properties: [A: 1])
        def b = new Neo4JNode(nodeType: "Person", conformedDimensions: [Id: 2], properties: [B: 1])
        def c = new Neo4JNode(nodeType: "Person", conformedDimensions: [Id: 3], properties: [A: 9])
        expect:
        a.signature() != b.signature()
        a.signature() == c.signature()
    }

    def "relationship direction is part of the connection signature"() {
        given:
        def s = new Neo4JNode(nodeType: "A", conformedDimensions: [Id: 1])
        def d = new Neo4JNode(nodeType: "B", conformedDimensions: [Id: 2])
        def forward = new Neo4JConnection(relationshipName: "R", source: s, destination: d, forwardRelationship: true)
        def reverse = new Neo4JConnection(relationshipName: "R", source: s, destination: d, forwardRelationship: false)
        expect:
        forward.signature() != reverse.signature()
    }

    def "whole numbers parse as Long, fractions as Double"() {
        given:
        def node = ConvertJSonToNeo4JObjects.fromJson('{"NodeType":"T","ConformedDimensions":{"Id":12345},"Properties":{"Ratio":1.5}}').first[0]
        expect:
        node.conformedDimensions.Id instanceof Long
        node.properties.Ratio instanceof Double
    }

    def "delete messages produce delete statements and skip connections"() {
        given:
        def result = ConvertJSonToNeo4JObjects.fromJson('{"NodeType":"T","ConformedDimensions":{"Id":1},"DeleteNode":true,"Connections":[{"NodeType":"U","RelType":"R","ForwardRel":true,"ConformedDimensions":{"Id":2}}]}')
        expect:
        result.first.size() == 1
        result.first[0].toCypher() == "MATCH (n:`T` {`Id`: param.`Id`}) DETACH DELETE n"
        result.second.isEmpty()
    }

    def "statements are pinned to Cypher 25"() {
        expect:
        Cypher.versioned("RETURN 1") == "CYPHER 25 RETURN 1"
    }
}
