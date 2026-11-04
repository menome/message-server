package com.menome.util

enum PreferenceType {
    RABBITMQ_HOST("localhost"),
    RABBITMQ_PORT("5672"),
    RABBITMQ_MANAGEMENT_PORT("15672"),
    RABBITMQ_USER("messageserver"),
    RABBITMQ_PASSWORD("messageserver"),
    RABBITMQ_QUEUE("test_queue"),
    RABBITMQ_ERROR_QUEUE(""),  // blank means "<RABBITMQ_QUEUE>.errors"
    RABBITMQ_EXCHANGE("test_exchange"),
    RABBITMQ_EXCHANGE_TYPE("direct"),
    RABBITMQ_ROUTE("test_route"),
    RABBITMQ_BATCH_SIZE("2000"),
    NEO4J_HOST("localhost"),
    NEO4J_BOLT_PORT("7687"),
    NEO4J_WEB_PORT("7474"),
    NEO4J_USER("neo4j"),
    NEO4J_PASSWORD("password"),
    RUN_WITH_TEST_CONTAINERS("Y"),
    SHOW_CONNECTION_LOG_OUTPUT("Y"),
    HTTP_SERVER_PORT("8081"),
    ENABLE_METRICS("N"),
    ENABLE_TEST_ENDPOINTS("N"),
    CASE_STYLE("camel"),
    // Names of the two properties the server writes when it creates a node. Blank: the default for CASE_STYLE (Uuid and
    // CreatedAt for camel). "none" leaves that property off. Any other value is used exactly as written, e.g. a graph
    // that already carries them under older names: ID_PROPERTY=Uuid CREATED_AT_PROPERTY=CreatedAt
    ID_PROPERTY(""),
    CREATED_AT_PROPERTY(""),
    // unique (default): uniqueness constraint on the dimension keys; nodekey: NODE KEY (Enterprise, falls back to unique);
    // index: plain range index only (no uniqueness guarantee; for graphs that already contain duplicates)
    KEY_CONSTRAINT("unique"),
    // Batches are parsed on PARSER_THREADS threads while WRITER_THREADS threads write earlier ones to Neo4j. One writer keeps
    // batches in arrival order (the last message for a node wins across batches); several do not.
    WRITER_THREADS("1"),
    PARSER_THREADS("1"),
    // phase: all relationship statements of a batch in one transaction (fewest commits). statement: one transaction per
    // statement, so a shared node (a hub) is locked only while its own statement runs and writers can work on different hubs at once
    RELATIONSHIP_TRANSACTIONS("phase")

    String defaultValue

    PreferenceType(String defaultValue) {
        this.defaultValue = defaultValue
    }
}
