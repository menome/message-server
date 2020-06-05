package com.menome.util

import com.menome.MessagingSpecification
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network

import java.time.Duration

abstract class MessagingWithTestContainersSpecification extends MessagingSpecification {

    static GenericContainer rabbitMQContainer
    static GenericContainer neo4JContainer

    def setupSpec() {
        // Start the containers if they need to be and haven't been started yet.
        if (ApplicationConfiguration.getString(PreferenceType.RUN_WITH_TEST_CONTAINERS) == "Y" && (!rabbitMQContainer || !neo4JContainer)) {
            startTestContainers()
        }
    }

    protected static void startTestContainers() {

        Network network = Network.newNetwork()

        log.info("Starting RabbitMQ Container")

        rabbitMQContainer = new GenericContainer("rabbitmq:management-alpine")
                .withNetwork(network)
                .withNetworkAliases("rabbitmq")
                .withExposedPorts(ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_PORT))
                .withExposedPorts(ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_MANAGEMENT_PORT))
                .withEnv("RABBITMQ_DEFAULT_USER", ApplicationConfiguration.getString(PreferenceType.RABBITMQ_USER))
                .withEnv("RABBITMQ_DEFAULT_PASS", ApplicationConfiguration.getString(PreferenceType.RABBITMQ_PASSWORD))

        rabbitMQContainer.start()

        System.setProperty(PreferenceType.RABBITMQ_PORT.name(), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_MANAGEMENT_PORT))

        log.info "RabbitMQ Docker container running at  - http://localhost:${rabbitMQContainer.getMappedPort(ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_MANAGEMENT_PORT))}"


        log.info("Starting Neo4J Container")
        neo4JContainer = new GenericContainer("neo4j:4.0.3")
                .withNetwork(network)
                .withNetworkAliases("neo4j")
                .withExposedPorts(ApplicationConfiguration.getInteger(PreferenceType.NEO4J_WEB_PORT))
                .withExposedPorts(ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT))
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withEnv("NEO4J_AUTH", "${ApplicationConfiguration.getString(PreferenceType.NEO4J_USER)}/${ApplicationConfiguration.getString(PreferenceType.NEO4J_PASSWORD)}")
                .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")
                .withStartupTimeout(Duration.ofMinutes(5))

        neo4JContainer.start()

        log.info "Neo4J Docker Container - Bolt bolt://localhost:${neo4JContainer.getMappedPort(ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT))}"
        log.info "Neo4J Docker Container - Web http://localhost:${neo4JContainer.getMappedPort(ApplicationConfiguration.getInteger(PreferenceType.NEO4J_WEB_PORT))}"

        System.setProperty(PreferenceType.NEO4J_BOLT_PORT.name(), neo4JContainer.getMappedPort(ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT)).toString())
        System.setProperty(PreferenceType.RABBITMQ_PORT.name(), rabbitMQContainer.getMappedPort(ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_PORT)).toString())
    }
}
