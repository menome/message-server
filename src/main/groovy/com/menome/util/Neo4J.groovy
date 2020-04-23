package com.menome.util


import org.neo4j.driver.*
import org.testcontainers.containers.GenericContainer

class Neo4J {

    static final int NEO4J_BOLT_API_PORT = 7687

    static Result run(Driver driver, String statement) {
        run(driver, statement, [:])
    }

    static Result run(Driver driver, List<String> statements, Map parameters) {
        Session session = driver.session()
        parameters.nodeParams = [:]
        def result = null
        statements.each() {
            result = session.run(it, parameters)
        }
        result
    }

    static Result run(Driver driver, String statement, Map parameters) {
        Session session = driver.session()
        def result = session.run(statement, parameters)
        return result
    }

    static Driver openDriver(GenericContainer neo4JContainer) {
        String boltPort = neo4JContainer.getMappedPort(NEO4J_BOLT_API_PORT) as String
        String boltURL = "bolt://localhost:$boltPort"
        return GraphDatabase.driver(boltURL, AuthTokens.basic("neo4j", "password"))
    }


    static executeStatementListInSession(List<String> statements, Session session) {
        String statement = ""
        statements.each() {
            statement += it + " \n"
        }
        session.writeTransaction(new TransactionWork() {
            @Override
            execute(Transaction tx) {
                tx.run(statement)
            }
        })
    }

}
