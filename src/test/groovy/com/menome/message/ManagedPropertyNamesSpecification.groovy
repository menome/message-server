package com.menome.message

import com.menome.messageParser.ConvertJSonToNeo4JObjects
import com.menome.util.PreferenceType
import spock.lang.Specification

/**
 * KANBAN E9-10: the names of the id and creation-timestamp properties the server writes are configuration, with neutral
 * defaults, so a graph that already carries the old names keeps them and a graph that wants neither gets neither.
 */
class ManagedPropertyNamesSpecification extends Specification {

    def cleanup() {
        [PreferenceType.CASE_STYLE, PreferenceType.ID_PROPERTY, PreferenceType.CREATED_AT_PROPERTY].each { System.clearProperty(it.name()) }
    }

    private static Neo4JNode person() { new Neo4JNode(nodeType: "Person", name: "Bob", conformedDimensions: [Id: 1]) }

    def "by default the server writes a neutral id and a creation timestamp, with no vendor name in the statement"() {
        expect:
        person().toCypher().contains("ON CREATE SET n.`Uuid` = randomUUID(), n.`CreatedAt` = datetime()")
        !person().toCypher().toLowerCase().contains("thelink")
    }

    def "the old names are reproduced by configuration"() {
        given:
        System.setProperty(PreferenceType.ID_PROPERTY.name(), "Uuid")
        System.setProperty(PreferenceType.CREATED_AT_PROPERTY.name(), "TheLinkAddedDate")

        expect:
        person().toCypher().contains("ON CREATE SET n.`Uuid` = randomUUID(), n.`TheLinkAddedDate` = datetime()")
    }

    def "an explicit name is used exactly as written, whatever the case style"() {
        given:
        System.setProperty(PreferenceType.CASE_STYLE.name(), "upper")
        System.setProperty(PreferenceType.CREATED_AT_PROPERTY.name(), "first_seen")

        expect:
        person().toCypher().contains("n.`UUID` = randomUUID(), n.`first_seen` = datetime()")
    }

    def "none switches a property off, and both off leaves no ON CREATE clause"() {
        when:
        System.setProperty(PreferenceType.CREATED_AT_PROPERTY.name(), "none")
        then:
        person().toCypher().contains("ON CREATE SET n.`Uuid` = randomUUID() SET")
        !person().toCypher().contains("datetime()")

        when:
        System.setProperty(PreferenceType.ID_PROPERTY.name(), "NONE")
        then:
        !person().toCypher().contains("ON CREATE")
        !person().toCypher().contains("randomUUID")
    }

    private static String message(Map props) {
        new com.google.gson.Gson().toJson([Name: "Ann", NodeType: "Employee", ConformedDimensions: [EmployeeId: 1], Properties: props])
    }

    def "a message may not set the configured names, and may set a name that is no longer managed"() {
        given:
        System.setProperty(PreferenceType.CREATED_AT_PROPERTY.name(), "TheLinkAddedDate")
        System.setProperty(PreferenceType.ID_PROPERTY.name(), "none")

        when: "the configured creation name is rejected"
        ConvertJSonToNeo4JObjects.fromJson(message([TheLinkAddedDate: "x"]))
        then:
        IllegalArgumentException e = thrown()
        e.message.contains("TheLinkAddedDate")

        when: "the default names are now ordinary properties"
        def parsed = ConvertJSonToNeo4JObjects.fromJson(message([CreatedAt: "x", Uuid: "y"]))
        then:
        parsed.v1*.properties == [[CreatedAt: "x", Uuid: "y"]]
    }
}
