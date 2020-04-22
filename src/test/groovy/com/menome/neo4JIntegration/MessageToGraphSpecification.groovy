package com.menome.neo4JIntegration

import com.menome.messageBuilder.Connection
import com.menome.messageBuilder.MessageBuilder
import com.menome.messageProcessor.MessageProcessor
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import spock.lang.Specification

import java.time.Duration

class MessageToGraphSpecification extends Specification {

    static final int NEO4J_BOLT_API_PORT = 7687
    static final int NEO4J_WEB_PORT = 7474

    static Logger log = LoggerFactory.getLogger(MessageToGraphSpecification.class)

    static String simpleMessage = MessageBuilder.builder()
            .Name("Konrad Aust")
            .NodeType("Employee")
            .Priority(1)
            .SourceSystem("HRSystem")
            .ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
            .build()
            .toJSON()

    static office = Connection.builder().Name("Menome Victoria").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).build()
    static project = Connection.builder().Name("theLink").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).build()
    static team = Connection.builder().Name("theLink Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).build()

    static String messageWithConnections = MessageBuilder.builder()
            .Name("Konrad Aust")
            .NodeType("Employee")
            .Priority(1)
            .SourceSystem("HRSystem")
            .ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
            .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
            .Connections([office, project, team])
            .build()
            .toJSON()



    protected static GenericContainer createAndStartNeo4JContainer(Network network) {
        GenericContainer neo4JContainer = new GenericContainer("neo4j:4.0.3")
                .withNetwork(network)
                .withNetworkAliases("neo4j")
                .withExposedPorts(NEO4J_WEB_PORT)
                .withExposedPorts(NEO4J_BOLT_API_PORT)
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withEnv("NEO4J_AUTH", "neo4j/password")
                .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")
                .withStartupTimeout(Duration.ofMinutes(5))

        neo4JContainer.start()

        log.info "Neo4J - Bolt bolt://localhost:${neo4JContainer.getMappedPort(NEO4J_BOLT_API_PORT)}"
        log.info "Neo4J - Web http://localhost:${neo4JContainer.getMappedPort(NEO4J_WEB_PORT)}"

        return neo4JContainer
    }

    /*

    https://neo4j.com/docs/driver-manual/current/session-api/simple/

               {
                    tx.run( "MATCH (emp:Person {name: $person_name}) " +
                            "MERGE (com:Company {name: $company_name}) " +
                            "MERGE (emp)-[:WORKS_FOR]->(com)",
                            parameters( "person_name", person.get( "name" ).asString(), "company_name",
                                    companyName ) );
                    return 1;

     */

    def "create graph from simple message"() {
        given:
        def neo4JContainer = createAndStartNeo4JContainer(Network.newNetwork())
        Driver driver = Neo4J.openDriver(neo4JContainer)
        Session session = driver.session()
        MessageProcessor processor = new MessageProcessor()
        when:

        processor.process(messageWithConnections)
        List<String> statements = processor.getNeo4JStatements()
        String statement = ""
        statements.each() {
            statement += it + " \n"
        }

        println "$statement"
        session.writeTransaction(new TransactionWork() {
            @Override
            execute(Transaction tx) {
                tx.run(statement)
            }
        })
        session.close()

        then:
        println "Pausing ...."
        sleep(10000000)
        //todo: Fetch bits from the graph

        1 == 1

    }
}
