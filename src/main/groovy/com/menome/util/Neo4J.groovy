package com.menome.util

import org.neo4j.driver.*
import org.neo4j.driver.internal.logging.JULogging
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer

import java.util.logging.Level


class Neo4J {

    static Logger log = LoggerFactory.getLogger(Neo4J.class)

    static Driver openDriver(GenericContainer neo4JContainer) {
        def host = ApplicationConfiguration.neo4JHost
        def boltPortEnv = ApplicationConfiguration.neo4JBoltPort.toInteger()
        String boltPort = neo4JContainer.getMappedPort(boltPortEnv)
        String boltURL = "bolt://$host:$boltPort"
        def build = Config.builder().withLogging(new JULogging(Level.OFF)).build()
        GraphDatabase.driver(boltURL, AuthTokens.basic(ApplicationConfiguration.neo4JUsername, ApplicationConfiguration.neo4JPassword), build)
    }

    static Driver openDriver() {
        def host = ApplicationConfiguration.neo4JHost
        def boltPort = ApplicationConfiguration.neo4JBoltPort
        def username = ApplicationConfiguration.neo4JUsername
        def password = ApplicationConfiguration.neo4JPassword
        String boltURL = "bolt://$host:$boltPort"
        log.info("Connecting to Neo4J server {} with user {}", boltURL, username)
        Config config = Config.builder().withLogging(new JULogging(Level.WARNING)).build()
        GraphDatabase.driver(boltURL, AuthTokens.basic(username, password), config)
    }


    static Result run(Driver driver, String statement) {
        run(driver, statement, [:])
    }

    static Result run(Driver driver, String statement, Map parameters) {
        Session session = driver.session()
        def result = session.run(statement, parameters)
        return result
    }


    static executeStatementListInSession(List<String> statements, Session session) {
        executeStatementListInSession(statements, session, [:])
    }

    static executeStatementListInSession(List<String> statements, Session session, Map parameters) {
        String statement = ""
        statements.each() {
            statement += it + " \n"
        }
        session.writeTransaction(new TransactionWork() {
            @Override
            execute(Transaction tx) {
                tx.run(statement, parameters)
            }
        })
    }

}
