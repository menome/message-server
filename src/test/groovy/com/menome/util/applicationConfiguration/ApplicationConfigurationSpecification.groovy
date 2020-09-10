package com.menome.util.applicationConfiguration

import com.menome.util.ApplicationConfiguration
import com.menome.util.PreferenceType
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



    def "test preference from environment variable"(){
        def newHost = "255.255.255.0"
        given:
        injectEnvironmentVariable(PreferenceType.RABBITMQ_HOST.name(), newHost)
        expect:
        newHost == ApplicationConfiguration.getString(PreferenceType.RABBITMQ_HOST)

    }

    // Super dirty hack to trick the JVM into setting environment variables. Only used in the context of this test
    // see:https://blog.sebastian-daschner.com/entries/changing_env_java

    private static void injectEnvironmentVariable(String key, String value)
            throws Exception {

        Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment")

        Field unmodifiableMapField = getAccessibleField(processEnvironment, "theUnmodifiableEnvironment")
        Object unmodifiableMap = unmodifiableMapField.get(null)
        injectIntoUnmodifiableMap(key, value, unmodifiableMap)

        Field mapField = getAccessibleField(processEnvironment, "theEnvironment")
        Map<String, String> map = (Map<String, String>) mapField.get(null)
        map.put(key, value)
    }

    private static Field getAccessibleField(Class<?> clazz, String fieldName)
            throws NoSuchFieldException {

        Field field = clazz.getDeclaredField(fieldName)
        field.setAccessible(true)
        return field
    }

    private static void injectIntoUnmodifiableMap(String key, String value, Object map)
            throws ReflectiveOperationException {

        Class unmodifiableMap = Class.forName("java.util.Collections\$UnmodifiableMap")
        Field field = getAccessibleField(unmodifiableMap, "m")
        Object obj = field.get(map)
        ((Map<String, String>) obj).put(key, value)
    }



}
