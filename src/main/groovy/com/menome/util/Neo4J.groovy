package com.menome.util

import org.neo4j.driver.*
import org.neo4j.driver.internal.logging.JULogging
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer

import java.util.concurrent.TimeUnit
import java.util.logging.Level

import static org.awaitility.Awaitility.await


class Neo4J {

    static Logger log = LoggerFactory.getLogger(Neo4J.class)

    static Driver openDriver(GenericContainer neo4JContainer) {
        def host = ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)
        def boltPortEnv = ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT)
        String boltPort = neo4JContainer.getMappedPort(boltPortEnv)
        String boltURL = "bolt://$host:$boltPort"
        def build = Config.builder().withLogging(new JULogging(Level.OFF)).build()
        GraphDatabase.driver(boltURL, AuthTokens.basic(ApplicationConfiguration.getString(PreferenceType.NEO4J_USER), ApplicationConfiguration.getString(PreferenceType.NEO4J_PASSWORD)), build)
    }

    static Driver openDriver() {
        def host = ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)
        def boltPort = ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT)
        def username = ApplicationConfiguration.getString(PreferenceType.NEO4J_USER)
        def password = ApplicationConfiguration.getString(PreferenceType.NEO4J_PASSWORD)
        String boltURL = "bolt://$host:$boltPort"
        if (ApplicationConfiguration.getString(PreferenceType.SHOW_CONNECTION_LOG_OUTPUT) == "Y") {
            log.info("Connecting to Neo4J server {} with user {}", boltURL, username)
        }
        Config config = Config.builder().withLogging(new JULogging(Level.WARNING)).build()

        GraphDatabase.driver(boltURL, AuthTokens.basic(username, password), config)
    }


    static Result run(Driver driver, String statement) {
        run(driver, statement, [:])
    }

    static Result run(Driver driver, String statement, Map parameters) {
        Session session = driver.session()
        def result = session.run(statement, parameters)
        result
    }


    static executeStatementListInSession(List<String> statements, Session session) {
        executeStatementListInSession(statements, session, [:])
    }

    static executeStatementListInSession(List<String> statements, Session session, Map parameters) {
        String statement = ""
        statements.each() {
            statement += it + " \n"
        }

        def retryCount = 2
        def retries = 0
        def success = false
        def lastException = null
        while (!success && retries < retryCount) {
            try {
                def transaction = session.beginTransaction()
                transaction.run(statement, parameters)
                transaction.commit()
                success = true
            } catch (Exception exception) {
                retries++
                lastException = exception
            }
        }
        if (!success) {
            throw lastException
        }
    }

    static void deleteAllTestNodes() {
        def driver = openDriver()
        run(driver, "match (n) where n.SourceSystem='menome_test_framework' detach delete n")
        await().atMost(1, TimeUnit.MINUTES).until { run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 0 }
    }


}
