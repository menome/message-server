package com.menome.message

import com.menome.util.PreferenceType
import spock.lang.Specification

/** KANBAN E2-1: signature()/identity() are computed once per object but must mean exactly what they always did. */
class SignatureCachingSpecification extends Specification {

    def cleanup() {
        System.clearProperty(PreferenceType.CASE_STYLE.name())
    }

    /** The definition of the signature, written out independently of the implementation's helper methods. */
    private static String expectedSignature(Neo4JNode n) {
        [n.deleteNode ? "delete" : "merge", n.nodeType, n.conformedDimensions.keySet().sort(false), n.propertyValues().keySet(),
         (n.dateProperties ?: [:]).keySet().sort(false), (n.deleteProperties ?: []).sort(false)].join("|")
    }

    private static List<Neo4JNode> shapes() {
        [
                new Neo4JNode(nodeType: "A", conformedDimensions: [Id: 1L]),
                new Neo4JNode(nodeType: "A", name: "n", priority: 1, sourceSystem: "s", label: "L", conformedDimensions: [Z: 1, A: 2], properties: [Status: "x", B: 1]),
                // A property that is also a dimension is a dimension only
                new Neo4JNode(nodeType: "A", name: "n", conformedDimensions: [Name: "n", Id: 1], properties: [Name: "n", Other: 2]),
                new Neo4JNode(nodeType: "B", conformedDimensions: [Id: 1], dateProperties: [Hired: "2026-01-01T00:00:00Z"], deleteProperties: ["Z", "A"]),
                new Neo4JNode(nodeType: "B", conformedDimensions: [Id: 1], deleteNode: true),
        ]
    }

    def "the cached signature is the same string the definition gives"() {
        expect:
        shapes().every { it.signature() == expectedSignature(it) }
    }

    def "the signature follows CASE_STYLE"() {
        given:
        System.setProperty(PreferenceType.CASE_STYLE.name(), style)

        expect:
        shapes().every { it.signature() == expectedSignature(it) }
        new Neo4JNode(nodeType: "A", name: "n", conformedDimensions: [Id: 1]).signature().contains(expectedName)

        where:
        style   | expectedName
        "lower" | "[name]"
        "upper" | "[NAME]"
        "camel" | "[Name]"
    }

    def "signature, identity and dimension keys are computed once per object"() {
        given:
        Neo4JNode node = shapes()[1]

        expect:
        node.signature().is(node.signature())
        node.identity().is(node.identity())
        node.dimensionKeys().is(node.dimensionKeys())
    }

    def "nodes of the same shape share one interned signature instance, different shapes do not"() {
        given:
        Neo4JNode a = new Neo4JNode(nodeType: "A", conformedDimensions: [Id: 1], properties: [P: 1])
        Neo4JNode b = new Neo4JNode(nodeType: "A", conformedDimensions: [Id: 2], properties: [P: 2])
        Neo4JNode c = new Neo4JNode(nodeType: "A", conformedDimensions: [Id: 2], properties: [Q: 2])

        expect:
        a.signature().is(b.signature())
        a.signature() != c.signature()
    }

    def "identity is the dimension values in key order, so key order in the message does not matter"() {
        expect:
        new Neo4JNode(nodeType: "A", conformedDimensions: [B: 2, A: 1]).identity() == [1, 2]
        new Neo4JNode(nodeType: "A", conformedDimensions: [A: 1, B: 2]).identity() == [1, 2]
    }

    def "the cached dimension keys cannot be modified by a caller"() {
        when:
        new Neo4JNode(nodeType: "A", conformedDimensions: [Id: 1]).dimensionKeys().add("x")

        then:
        thrown(UnsupportedOperationException)
    }

    def "a connection caches its signature and identity too, and the identity tells connections apart"() {
        given:
        Neo4JNode from = new Neo4JNode(nodeType: "A", conformedDimensions: [Id: 1])
        Neo4JConnection one = new Neo4JConnection(relationshipName: "R", source: from, destination: new Neo4JNode(nodeType: "B", conformedDimensions: [Id: 1]), forwardRelationship: true)
        Neo4JConnection two = new Neo4JConnection(relationshipName: "R", source: from, destination: new Neo4JNode(nodeType: "B", conformedDimensions: [Id: 2]), forwardRelationship: true)
        Neo4JConnection reversed = new Neo4JConnection(relationshipName: "R", source: from, destination: new Neo4JNode(nodeType: "B", conformedDimensions: [Id: 1]), forwardRelationship: false)

        expect:
        one.signature().is(one.signature())
        one.identity().is(one.identity())
        one.signature().is(two.signature())
        one.identity() != two.identity()
        one.signature() != reversed.signature()
    }
}
