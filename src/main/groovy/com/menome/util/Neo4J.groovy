package com.menome.util

import org.neo4j.driver.*
import org.neo4j.driver.internal.logging.JULogging
import org.neo4j.driver.summary.ResultSummary
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import java.util.logging.Level

import static org.awaitility.Awaitility.await

class Neo4J {

    static Logger log = LoggerFactory.getLogger(Neo4J.class)
    static boolean connectionInformationDisplayed = Boolean.FALSE

    static boolean connectionOk() {
        boolean connectionOk
        try {
            executeDbmsComponents()
            connectionOk = true
        } catch (Exception ignored) {
            connectionOk = false
        }
        connectionOk
    }

    static String version() {
        Record record = executeDbmsComponents()
        record.get("versions").values()[0].asString()
    }

    static String edition() {
        Record record = executeDbmsComponents()
        record.get("edition").asString()
    }


    private static Record executeDbmsComponents() {
        Driver driver = openDriver()
        def result = run(driver, "call dbms.components() yield name, versions, edition")
        def record = result.single()
        record
    }


    static Driver openDriver() {
        def host = ApplicationConfiguration.getString(PreferenceType.NEO4J_HOST)
        def boltPort = ApplicationConfiguration.getInteger(PreferenceType.NEO4J_BOLT_PORT)
        def username = ApplicationConfiguration.getString(PreferenceType.NEO4J_USER)
        def password = ApplicationConfiguration.getString(PreferenceType.NEO4J_PASSWORD)
        def protocol = ""
        if (!host.contains(":")) {
            protocol = "bolt://"
        }
        String boltURL = "$protocol$host:$boltPort"
        // Only display the log message once per "session"
        if ((ApplicationConfiguration.getString(PreferenceType.SHOW_CONNECTION_LOG_OUTPUT) == "Y") && !connectionInformationDisplayed) {
            log.info("Connecting to Neo4J server {} with user {}", boltURL, username)
            connectionInformationDisplayed = true
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

    static ResultSummary executeStatementListInSession(List<String> statements, Session session, Map parameters) {
        String statement = ""
        statements.each() {
            statement += it + " \n"
        }

        def retryCount = 2
        def retries = 0
        def success = false
        def lastException = null
        ResultSummary resultSummary = null
        while (!success && retries < retryCount) {
            def transaction = session.beginTransaction()
            try {
                def result = transaction.run(statement, parameters)
                resultSummary = result.consume()
                transaction.commit()
                success = true
            } catch (Exception exception) {
                transaction.rollback()
                retries++
                lastException = exception
            } finally {
                transaction.close()
            }
        }
        if (!success) {
            throw lastException
        }
        resultSummary
    }

    static void deleteAllTestNodes() {
        def driver = openDriver()
        run(driver, "match (n) where n.SourceSystem='menome_test_framework' detach delete n")
        await().atMost(1, TimeUnit.MINUTES).until { run(driver, "match (n) where n.SourceSystem='menome_test_framework' return count(n) as count").single().get("count").asInt() == 0 }
    }


}
