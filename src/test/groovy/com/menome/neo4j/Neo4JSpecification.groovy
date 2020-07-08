package com.menome.neo4j

import com.menome.util.ApplicationConfiguration
import com.menome.util.Neo4J
import com.menome.util.PreferenceType
import spock.lang.Specification

class Neo4JSpecification extends Specification {

    def "connection ok"() {
        given:
        def connectionOk = Neo4J.connectionOk()
        expect:
        Boolean.TRUE == connectionOk
    }

    def "connection not ok"() {
        given:
        def originalURL = ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)
        System.setProperty(PreferenceType.NEO4J_HOST.toString(), "badhostname")
        def connectionOk = Neo4J.connectionOk()
        expect:
        Boolean.FALSE == connectionOk
        cleanup:
        System.setProperty(PreferenceType.NEO4J_HOST.toString(), originalURL)
    }

    def "url with protocol"() {
        given:
        def originalURL = ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)
        System.setProperty(PreferenceType.NEO4J_HOST.toString(), "bolt://localhost")
        expect:
        Boolean.TRUE == Neo4J.connectionOk()
        cleanup:
        System.setProperty(PreferenceType.NEO4J_HOST.toString(), originalURL)

    }

    def "url without protocol"() {
        given:
        def originalURL = ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)
        System.setProperty(PreferenceType.NEO4J_HOST.toString(), "localhost")
        expect:
        Boolean.TRUE == Neo4J.connectionOk()
        cleanup:
        System.setProperty(PreferenceType.NEO4J_HOST.toString(), originalURL)

    }

    def "neo4j edition valid"() {
        given:
        def edition = Neo4J.edition()
        expect:
        edition
        ["community", "enterprise"].contains(edition)
    }

    def "neo4j version valid"() {
        given:
        def version = Neo4J.version()
        expect:
        version
        version.startsWith("4")
    }

}
