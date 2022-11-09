package com.menome.util.applicationConfiguration

import com.menome.util.ApplicationConfiguration
import com.menome.util.PreferenceType
import spock.lang.Ignore
import spock.lang.Specification

import java.lang.reflect.Field

class ApplicationConfigurationSpecification extends Specification {

    def "test defaults"() {
        given:
        System.setProperties(new Properties())
        expect:
        "neo4j" == ApplicationConfiguration.getString(PreferenceType.NEO4J_USER)
        "localhost" == ApplicationConfiguration.getString(PreferenceType.RABBITMQ_HOST)
        5672 == ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_PORT)
        "menome" == ApplicationConfiguration.getString(PreferenceType.RABBITMQ_USER)
        "menome" == ApplicationConfiguration.getString(PreferenceType.RABBITMQ_PASSWORD)
        "test_queue" ==ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE)
        "test_exchange" == ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE)
        "test_route" == ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE)
        500 == ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_BATCH_SIZE)
        "localhost" == ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)
        7687 == ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT)
        "neo4j" == ApplicationConfiguration.getString(PreferenceType.NEO4J_USER)
        "password" == ApplicationConfiguration.getString(PreferenceType.NEO4J_PASSWORD)
    }

    def "test preference from system property"() {
        given:
        def newHost = "255.255.255.255"
        System.setProperty(PreferenceType.RABBITMQ_HOST.name(),newHost)
        expect:
        newHost == ApplicationConfiguration.getString(PreferenceType.RABBITMQ_HOST)
    }


}
