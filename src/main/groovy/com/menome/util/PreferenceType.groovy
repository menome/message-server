package com.menome.util;

enum PreferenceType {
    RABBITMQ_HOST("localhost"),
    RABBITMQ_PORT("5672"),
    RABBITMQ_MANAGEMENT_PORT("15672"),
    RABBITMQ_USER("menome"),
    RABBITMQ_PASSWORD("menome"),
    RABBITMQ_QUEUE("test_queue"),
    RABBITMQ_EXCHANGE("test_exchange"),
    RABBITMQ_ROUTE("test_route"),
    RABBITMQ_BATCH_SIZE("500"),
    NEO4J_HOST("localhost"),
    NEO4J_BOLT_PORT("7687"),
    NEO4J_WEB_PORT("7474"),
    NEO4J_USER("neo4j"),
    NEO4J_PASSWORD("password"),
    REDIS_HOST("localhost"),
    REDIS_PORT("6379"),
    RUN_WITH_TEST_CONTAINERS("Y"),
    USE_REDIS_CACHE("Y"),
    SHOW_CONNECTION_LOG_OUTPUT("Y"),
    HTTP_SERVER_PORT("8081")


    String defaultValue;

    PreferenceType(String defaultValue) {
        this.defaultValue = defaultValue;
    }
}
