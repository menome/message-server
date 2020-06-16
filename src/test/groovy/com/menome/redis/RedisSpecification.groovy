package com.menome.redis

import com.menome.util.ApplicationConfiguration
import com.menome.util.PreferenceType
import com.menome.util.Redis
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import spock.lang.Specification

class RedisSpecification extends Specification {

    static Logger log = LoggerFactory.getLogger(RedisSpecification.class)

    static GenericContainer redisContainer


    def setupSpec() {
        // Start the containers if they need to be and haven't been started yet.
        if (ApplicationConfiguration.getString(PreferenceType.RUN_WITH_TEST_CONTAINERS) == "N") {
            // Nothing to do here. We're assuming the containers are running outside the test or we're testing against external services that are configured with the environment variables.
        } else if (!redisContainer) {
            startTestContainers()
        }
    }


    def "test redis connection ok"() {
        given:
        def ok = Redis.connectionOk()
        expect:
        ok
    }


    def "test redis set and get key"() {
        given:
        def connection = Redis.connection()
        connection.set("key", "Hello, Redis!")

        expect:
        "Hello, Redis!" == connection.get("key")
        cleanup:
        connection.close()
    }

    def "test redis flush all"() {
        given:
        def connection = Redis.connection()
        connection.set("key", "Hello, Redis!")
        Redis.clearCache()
        expect:
        connection.dbSize() == 0
        cleanup:
        connection.close()
    }

    protected static void startTestContainers() {

        Network network = Network.newNetwork()

        log.info("Starting RabbitMQ Container")

        redisContainer = new GenericContainer("redis")
                .withNetwork(network)
                .withNetworkAliases("redis")
                .withExposedPorts(ApplicationConfiguration.getInteger(PreferenceType.REDIS_PORT))

        redisContainer.start()

        System.setProperty(PreferenceType.REDIS_PORT.name(), redisContainer.getMappedPort(ApplicationConfiguration.getInteger(PreferenceType.REDIS_PORT)).toString())

        log.info "Redis Docker container running at  - http://localhost:${ApplicationConfiguration.getInteger(PreferenceType.REDIS_PORT)}"
    }
}
