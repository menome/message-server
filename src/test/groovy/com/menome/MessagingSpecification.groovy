package com.menome

import com.menome.messageBuilder.Connection
import com.menome.messageBuilder.MessageBuilder
import com.menome.rabbitIntegration.RabbitMQVolumeSpecification
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import spock.lang.Specification

import java.time.Duration
import java.time.LocalDateTime
import java.time.Month

class MessagingSpecification extends Specification {

    protected static final int NEO4J_BOLT_API_PORT = 7687
    protected static final int NEO4J_WEB_PORT = 7474

    protected static final int RABBITMQ_PORT = 5672
    protected static final int RABBITMQ_MANAGEMENT_PORT = 15672

    protected static final String RABBITMQ_TEST_EXCHANGE = "test_exchange"
    protected static final String RABBITMQ_TEST_ROUTING_KEY = "test_route"
    protected static final String RABBITMQ_QUEUE_NAME = "test_queue"

    static Logger log = LoggerFactory.getLogger(MessagingSpecification.class)

    protected static String simpleMessage = MessageBuilder.builder()
            .Name("Konrad Aust")
            .NodeType("Employee")
            .Priority(1)
            .SourceSystem("HRSystem")
            .ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
            .build()
            .toJSON()


    protected static office = Connection.builder().Name("Menome Victoria").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).build()
    protected static project = Connection.builder().Name("theLink").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).build()
    protected static team = Connection.builder().Name("theLink Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).build()

    protected static String employeeMessageWithConnections = buildEmployeeMessageWithConnections(false)

    protected static String buildEmployeeMessageWithConnections(boolean generateUniqeId) {
        MessageBuilder.builder()
                .Name("Konrad Aust")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("HRSystem")
                .ConformedDimensions("Email": "konrad.aust" + (generateUniqeId ? UUID.randomUUID() : "") + "@menome.com", "EmployeeId": 12345)
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .Connections([office, project, team])
                .build()
                .toJSON()
    }

    protected static alice = Connection.builder().Name("Alice").NodeType("AdvisoryBoardMember").RelType("Organizer").ForewardRel(true).ConformedDimensions(["id": UUID.randomUUID()]).build()
    protected static bob = Connection.builder().Name("Bob").NodeType("AdvisoryBoardMember").RelType("Participant").ForewardRel(true).ConformedDimensions(["id": UUID.randomUUID()]).build()
    protected static charlie = Connection.builder().Name("Charlie").NodeType("AdvisoryBoardMember").RelType("Participant").ForewardRel(true).ConformedDimensions(["id": UUID.randomUUID()]).build()
    protected static String meetingMessageWithConnections = MessageBuilder.builder()
            .Name("Advisory Board Meeting")
            .NodeType("Meeting")
            .Priority(1)
            .SourceSystem("Outlook")
            .ConformedDimensions("MeetingId": UUID.randomUUID())
            .Properties(["Status": "active", "Location": "Boardroom", "ScheduledDate": LocalDateTime.of(2020, Month.JANUARY, 25, 9, 30).toString()])
            .Connections([alice, bob, charlie])
            .build()
            .toJSON()

    protected static String invalidMessage = MessageBuilder.builder()
            .Name("Bad Message")
            .NodeType("Invalid Node With Spaces")
            .Priority(1)
            .SourceSystem("Bad Actor")
            .ConformedDimensions("Id": 1)
            .Properties(["Status": "active"])
            .build()
            .toJSON()


    protected static List<String> threeMessageBatch = (1..3).collect() {
        buildEmployeeMessageWithConnections(true)
    }

    protected static List<String> twoMessagesDifferentTypeBatch = [
            employeeMessageWithConnections,
            meetingMessageWithConnections]


    protected static List<String> fiveThousandMessageBatch = (1..5000).collect() {
        MessageBuilder.builder()
                .Name("Konrad Aust")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("HRSystem")
                .ConformedDimensions("Email": "konrad.aust" + UUID.randomUUID() + "@menome.com", "EmployeeId": UUID.randomUUID())
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .Connections([office, project, team])
                .build()
                .toJSON()
    }

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

        println "Neo4J - Bolt bolt://localhost:${neo4JContainer.getMappedPort(NEO4J_BOLT_API_PORT)}"
        println "Neo4J - Web http://localhost:${neo4JContainer.getMappedPort(NEO4J_WEB_PORT)}"

        return neo4JContainer
    }

    protected static ConnectionFactory createRabbitConnectionFactory() {
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory()
        rabbitConnectionFactory.host = "127.0.0.1"
        rabbitConnectionFactory.port = RABBITMQ_PORT
        rabbitConnectionFactory.username = "menome"
        rabbitConnectionFactory.password = "menome"

        return rabbitConnectionFactory
    }

    protected static GenericContainer createAndStartRabbitMQContainer(Network network) {
        GenericContainer rabbitMQContainer = new GenericContainer("rabbitmq:management-alpine")
                .withNetwork(network)
                .withNetworkAliases("rabbitmq")
                .withExposedPorts(RABBITMQ_PORT)
                .withExposedPorts(RABBITMQ_MANAGEMENT_PORT)
                .withEnv("RABBITMQ_DEFAULT_USER", "menome")
                .withEnv("RABBITMQ_DEFAULT_PASS", "menome")

        rabbitMQContainer.start()

        RabbitMQVolumeSpecification.log.info "Rabbit MQ - http://localhost:${rabbitMQContainer.getMappedPort(RABBITMQ_MANAGEMENT_PORT)}"

        return rabbitMQContainer
    }

    protected static Channel openRabbitMQChanel(String queue, String exchange, String routingKey, ConnectionFactory rabbitConnectionFactory) {

        com.rabbitmq.client.Connection rabbitConnection = rabbitConnectionFactory.newConnection()
        Channel rabbitChannel = rabbitConnection.createChannel()
        rabbitChannel.queueDeclare(queue, true, false, false, null)
        rabbitChannel.exchangeDeclare(exchange, "topic", true)
        rabbitChannel.queueBind(queue, exchange, routingKey)

        return rabbitChannel
    }

}
