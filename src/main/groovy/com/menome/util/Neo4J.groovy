package com.menome.util

import com.menome.message.Neo4JNode
import org.neo4j.driver.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4J {

    static Logger log = LoggerFactory.getLogger(Neo4J.class)
    static boolean connectionInformationDisplayed = Boolean.FALSE

    static boolean connectionOk() {
        try {
            withDriver { Driver driver -> driver.verifyConnectivity() }
            true
        } catch (Exception ignored) {
            false
        }
    }

    static String version() {
        dbmsComponents().get("versions").values()[0].asString()
    }

    static String edition() {
        dbmsComponents().get("edition").asString()
    }

    private static Record dbmsComponents() {
        withDriver { Driver driver ->
            driver.session().withCloseable { Session session ->
                session.run("CALL dbms.components() YIELD name, versions, edition WHERE name = 'Neo4j Kernel' RETURN versions, edition").single()
            }
        } as Record
    }

    /** Opens a driver, runs the closure and always closes the driver. */
    static <T> T withDriver(Closure<T> work) {
        Driver driver = openDriver()
        try {
            work(driver)
        } finally {
            driver.close()
        }
    }

    /** The caller owns the returned driver and must close it. A driver is a connection pool: share one per process. */
    static Driver openDriver() {
        def host = ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)
        def boltPort = ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT)
        def username = ApplicationConfiguration.getString(PreferenceType.NEO4J_USER)
        def password = ApplicationConfiguration.getString(PreferenceType.NEO4J_PASSWORD)
        // A host that already carries a scheme (neo4j+s://...) is used as is
        String boltURL = host.contains("://") ? "$host:$boltPort" : "bolt://$host:$boltPort"
        // Only display the log message once per "session"
        if ((ApplicationConfiguration.getString(PreferenceType.SHOW_CONNECTION_LOG_OUTPUT) == "Y") && !connectionInformationDisplayed) {
            log.info("Connecting to Neo4J server {} with user {}", boltURL, username)
            connectionInformationDisplayed = true
        }
        GraphDatabase.driver(boltURL, AuthTokens.basic(username, password))
    }

    static Result run(Driver driver, String statement) {
        run(driver, statement, [:])
    }

    static Result run(Driver driver, String statement, Map parameters) {
        run(driver.session(), statement, parameters)
    }

    static Result run(Session session, String statement, Map parameters) {
        session.run(statement, parameters)
    }

    static void deleteAllTestNodes() {
        withDriver { Driver driver ->
            driver.session().withCloseable { Session session ->
                String property = Cypher.id(Neo4JNode.keys().sourceSystem)
                session.run(Cypher.versioned("MATCH (n) WHERE n.${property} = 'menome_test_framework' CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS")).consume()
            }
        }
    }
}
