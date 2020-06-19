package com.menome


import com.menome.util.PreferenceType
import com.menome.util.RabbitMQ
import com.menome.util.messageBuilder.Connection
import com.menome.util.messageBuilder.MessageBuilder
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.time.LocalDateTime
import java.time.Month

abstract class MessagingSpecification extends Specification {
    {
        System.setProperty(PreferenceType.SHOW_CONNECTION_LOG_OUTPUT.name(), "N")
    }


    static Logger log = LoggerFactory.getLogger(MessagingSpecification.class)

    protected static String simpleMessage = MessageBuilder.builder()
            .Name("Konrad Aust")
            .NodeType("Employee")
            .Priority(1)
            .SourceSystem("menome_test_framework")
            .ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
            .build()
            .toJSON()


    protected static victoriaOffice = Connection.builder().Name("Menome Victoria").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).Properties(["SourceSystem": "menome_test_framework"]).build()
    protected static calgaryOffice = Connection.builder().Name("Menome Calgary").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Calgary"]).Properties(["SourceSystem": "menome_test_framework"]).build()
    protected static project = Connection.builder().Name("theLink").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).Properties(["SourceSystem": "menome_test_framework"]).build()
    protected static team = Connection.builder().Name("theLink Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).Properties(["SourceSystem": "menome_test_framework"]).build()

    protected static String victoriaEmployee = buildVictoriaEmployeeMessage(false)
    protected static String calgaryEmployee = buildCalgaryEmployeeMessage(false)

    protected static String buildVictoriaEmployeeMessage(boolean generateUniqeId) {
        buildEmployeeMessage(generateUniqeId, victoriaOffice)
    }

    protected static buildCalgaryEmployeeMessage(boolean generateUniqueId) {
        buildEmployeeMessage(generateUniqueId, calgaryOffice)
    }

    protected static String buildEmployeeMessage(boolean generateUniqeId, Connection officeConnection) {
        MessageBuilder.builder()
                .Name("Konrad Aust")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("menome_test_framework")
                .ConformedDimensions("Email": "konrad.aust" + (generateUniqeId ? UUID.randomUUID() : "") + "@menome.com", "EmployeeId": 12345)
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .Connections([officeConnection, project, team])
                .build()
                .toJSON()
    }

    protected static alice = Connection.builder().Name("Alice").NodeType("AdvisoryBoardMember").RelType("Organizer").ForewardRel(true).ConformedDimensions(["id": UUID.randomUUID()]).Properties(["SourceSystem": "menome_test_framework"]).build()
    protected static bob = Connection.builder().Name("Bob").NodeType("AdvisoryBoardMember").RelType("Participant").ForewardRel(true).ConformedDimensions(["id": UUID.randomUUID()]).Properties(["SourceSystem": "menome_test_framework"]).build()
    protected static charlie = Connection.builder().Name("Charlie").NodeType("AdvisoryBoardMember").RelType("Participant").ForewardRel(true).ConformedDimensions(["id": UUID.randomUUID()]).Properties(["SourceSystem": "menome_test_framework"]).build()
    protected static String meetingMessageWithConnections = MessageBuilder.builder()
            .Name("Advisory Board Meeting")
            .NodeType("Meeting")
            .Priority(1)
            .SourceSystem("menome_test_framework")
            .ConformedDimensions("MeetingId": UUID.randomUUID())
            .Properties(["Status": "active", "Location": "Boardroom", "ScheduledDate": LocalDateTime.of(2020, Month.JANUARY, 25, 9, 30).toString()])
            .Connections([alice, bob, charlie])
            .build()
            .toJSON()

    protected static String invalidMessage = MessageBuilder.builder()
            .Name("Bad Message")
            .NodeType("Invalid Node With Spaces")
            .Priority(1)
            .SourceSystem("menome_test_framework")
            .ConformedDimensions("Id": 1)
            .Properties(["Status": "active"])
            .build()
            .toJSON()

    protected static badConnection = Connection.builder().Name("Bad Connection").NodeType("Bad Connection").RelType("Bad").ForewardRel(true).ConformedDimensions(["id": UUID.randomUUID()]).build()
    protected static String validMessageWithInvalidConnection = MessageBuilder.builder()
            .Name("Good Message")
            .NodeType("GoodNode")
            .Priority(1)
            .SourceSystem("menome_test_framework")
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


    protected static ConnectionFactory createRabbitConnectionFactory() {
        RabbitMQ.createRabbitConnectionFactory()
    }

    protected static Channel openRabbitMQChanel(String queue, String exchange, String routingKey, ConnectionFactory rabbitConnectionFactory) {
        return RabbitMQ.openRabbitMQChannel(queue, exchange, routingKey, rabbitConnectionFactory)
    }

}
