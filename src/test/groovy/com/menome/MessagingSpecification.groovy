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
import java.time.Instant
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


    protected static victoriaOffice = Connection.builder().Name("Menome Victoria").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).build()
    protected static calgaryOffice = Connection.builder().Name("Menome Calgary").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Calgary"]).build()
    protected static project = Connection.builder().Name("theLink").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).build()
    protected static team = Connection.builder().Name("theLink Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).build()

    protected static String victoriaEmployee = buildVictoriaEmployeeMessage(false)
    protected static String calgaryEmployee = buildCalgaryEmployeeMessage(false)

    protected static String buildVictoriaEmployeeMessage(boolean generateUniqeId){
        buildEmployeeMessage(generateUniqeId,victoriaOffice)
    }

    protected static buildCalgaryEmployeeMessage(boolean generateUniqueId){
        buildEmployeeMessage(generateUniqueId,calgaryOffice)
    }
    protected static String buildEmployeeMessage(boolean generateUniqeId, Connection officeConnection) {
        MessageBuilder.builder()
                .Name("Konrad Aust")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("HRSystem")
                .ConformedDimensions("Email": "konrad.aust" + (generateUniqeId ? UUID.randomUUID() : "") + "@menome.com", "EmployeeId": 12345)
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .Connections([officeConnection, project, team])
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

    protected static badConnection = Connection.builder().Name("Bad Connection").NodeType("Bad Connection").RelType("Bad").ForewardRel(true).ConformedDimensions(["id": UUID.randomUUID()]).build()
    protected static String validMessageWithInvalidConnection = MessageBuilder.builder()
            .Name("Good Message")
            .NodeType("GoodNode")
            .Priority(1)
            .SourceSystem("Bad Actor")
            .ConformedDimensions("Id": 1)
            .Properties(["Status": "active"])
            .Connections([badConnection])
            .build()
            .toJSON()


    protected static List<String> threeMessageBatch = (1..3).collect() {
        buildVictoriaEmployeeMessage(true)
    }

    protected static List<String> twoMessagesDifferentTypeBatch = [
            victoriaEmployee,
            meetingMessageWithConnections]


    protected static List<String> fiveThousandMessageBatch = (1..5000).collect() {
        MessageBuilder.builder()
                .Name("Konrad Aust")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("HRSystem")
                .ConformedDimensions("Email": "konrad.aust" + UUID.randomUUID() + "@menome.com", "EmployeeId": UUID.randomUUID())
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .Connections([victoriaOffice, project, team])
                .build()
                .toJSON()
    }



    List<String> buildSymendMessages(int count) {
        int ACCOUNT_COUNT = 5000
        int ACTIVITY_COUNT = 10
        int DIALER_COUNT = 5000
        List<String> messages = []
        Random random = new Random()
        def today = Instant.now().toString()
        List<Connection> accounts = buildAccountList(ACCOUNT_COUNT)
        List<Connection> activities = buildActivityList(ACTIVITY_COUNT)
        List<Connection> dialers = buildDialerList(DIALER_COUNT)
        (1..count).each {
            def message = MessageBuilder.builder()
                    .Name("Collection:+${UUID.randomUUID()}")
                    .NodeType("CollectionEvent")
                    .Priority(1)
                    .SourceSystem("MOBILE_STAGE__V_ADF_COLLECTION_CURR")
                    .ConformedDimensions("ACCTNUM": random.nextInt(5000), "ENT_SEQ_NO": random.nextInt(5000))
                    .Properties([
                            "VENDOR_SDC"              : UUID.randomUUID(),
                            "APPLICATION_ID"          : UUID.randomUUID(),
                            "DL_SERVICE_CODE"         : UUID.randomUUID(),
                            "ASGN_COLLECTOR"          : UUID.randomUUID(),
                            "ASGN_AGENCY"             : UUID.randomUUID(),
                            "APPROVAL_COLLECTOR"      : UUID.randomUUID(),
                            "COL_PATH_CODE"           : UUID.randomUUID(),
                            "COL_ACTV_CODE"           : UUID.randomUUID(),
                            "COL_ACTV_TYPE_IND"       : UUID.randomUUID(),
                            "COL_WAIVER_IND"          : UUID.randomUUID(),
                            "COL_COLLECTOR_ID"        : UUID.randomUUID(),
                            "COL_NEXT_STP_APR_COD"    : UUID.randomUUID(),
                            "COL_ACTV_RSN_CODE"       : UUID.randomUUID(),
                            "FRAUD_TREATMENT_IND"     : UUID.randomUUID(),
                            "COL_ASSIGNED_GRP"        : UUID.randomUUID(),
                            "COL_TYPE"                : UUID.randomUUID(),
                            "PA_CATEGORY"             : UUID.randomUUID(),
                            "OUTSRC_AGENCY"           : UUID.randomUUID(),
                            "COL_WO_REASON"           : UUID.randomUUID(),
                            "COL_SMS_LTR_CODE"        : UUID.randomUUID(),
                            "COL_WAIVER_RSN_CODE"     : UUID.randomUUID(),
                            "COLLECTION_TIMED_ACTION1": UUID.randomUUID(),
                            "COLLECTION_TIMED_ACTION2": UUID.randomUUID(),
                            "COLLECTION_TIMED_ACTION3": UUID.randomUUID(),
                            "COLLECTION_TIMED_ACTION4": UUID.randomUUID(),
                            "COLLECTION_TIMED_ACTION5": UUID.randomUUID(),
                            "BAN"                     : random.nextInt(100),
                            "ENT_SEQ_NO"              : random.nextInt(100),
                            "OPERATOR_ID"             : random.nextInt(100),
                            "DL_UPDATE_STAMP"         : random.nextInt(100),
                            "COL_STEP_NUM"            : random.nextInt(100),
                            "SYS_CREATION_DATE"       : today,
                            "SYS_UPDATE_DATE"         : today,
                            "COL_ACTV_DATE"           : today,
                            "ASGN_COLL_DATE"          : today,
                            "ASGN_AGENCY_DATE"        : today,
                            "COL_NEXT_STEP_DATE"      : today,
                            "FRAUD_TREATMENT_DATE"    : today,
                            "COL_WAIVER_EXP_DATE"     : today,
                            "COL_ACTV_DATE_OLD"       : today,
                            "SYS_DATE"                : today
                    ])
                    .Connections([
                            accounts[random.nextInt(ACCOUNT_COUNT)]
                            , activities[random.nextInt(ACTIVITY_COUNT)]
                            , dialers[random.nextInt(DIALER_COUNT)]])
                    .build()
                    .toJSON()

            messages.add(message)
        }
        messages
    }

    List<Connection> buildAccountList(int count) {
        Random random = new Random()
        List<Connection> connections = []
        (1..count).each {
            def account = Connection.builder().Name("Account ${UUID.randomUUID()}").NodeType("Account").RelType("COLLECTION_ENTRY_FOR_ACCOUNT").ForewardRel(true).ConformedDimensions(["ACCTNUM": random.nextInt(count)]).build()
            connections.add(account)
        }
        connections
    }

    List<Connection> buildActivityList(count) {
        Random random = new Random()
        List<Connection> connections = []
        (1..count).each {
            def activity = Connection.builder().Name("Activity ${UUID.randomUUID()}").NodeType("CollectionActivity").RelType("ACTIVITY").ForewardRel(true).ConformedDimensions(["COL_ACTIVITY_CODE": random.nextInt(count)]).build()
            connections.add(activity)
        }
        connections
    }

    List<Connection> buildDialerList(count) {
        Random random = new Random()
        List<Connection> connections = []
        (1..count).each {
            def activity = Connection.builder().Name("Dialer Entry ${UUID.randomUUID()}").NodeType("DialerEntry").RelType("COLLECTION").ForewardRel(true).ConformedDimensions(["SNAPSHOT_SDT": random.nextInt(count), "ACCTNUM": random.nextInt(count)]).build()
            connections.add(activity)
        }
        connections
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
