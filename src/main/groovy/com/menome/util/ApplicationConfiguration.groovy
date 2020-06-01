package com.menome.util

class ApplicationConfiguration {
    //RabbitMQ Configuration
    static final String rabbitMQHost = Optional.ofNullable(System.getenv("RABBITMQ_HOST")).orElse("localhost")
    static final Integer rabbitMQPort = Optional.ofNullable(System.getenv("RABBITMQ_PORT")).orElse("5672").toInteger()
    static final String rabbitMQUsername = Optional.ofNullable(System.getenv("RABBITMQ_USER")).orElse("menome")
    static final String rabbitMQPassword = Optional.ofNullable(System.getenv("RABBITMQ_PASSWORD")).orElse("menome")
    static final String rabbitMQQueue = Optional.ofNullable(System.getenv("RABBITMQ_QUEUE")).orElse("test_queue")
    static final String rabbitMQExchange = Optional.ofNullable(System.getenv("RABBITMQ_EXCHANGE")).orElse("test_exchange") //todo: It would be nice to get rid of the exchange declaration. The rabbit-reactor library seems to have a way of connecting to the queue without the exchange
    static final String rabbitMQRoute = Optional.ofNullable(System.getenv("RABBITMQ_ROUTE")).orElse("test_route") //todo: It would be nice to get rid of the route declaration.

    static final Integer rabbitMQBatchSize = Optional.ofNullable(System.getenv("RABBITMQ_BATCHSZIE")).orElse("5000").toInteger()

    //Neo4J Configuration
    static final String neo4JHost = Optional.ofNullable(System.getenv("NEO4J_HOST")).orElse("localhost")
    static final String neo4JBoltPort = Optional.ofNullable(System.getenv("NEO4J_BOLT_PORT")).orElse("7687")
    static final String neo4JUsername = Optional.ofNullable(System.getenv("NEO4J_USER")).orElse("neo4j")
    static final String neo4JPassword = Optional.ofNullable(System.getenv("NEO4J_PASSWORD")).orElse("password")
}
