package com.menome.messageServerEndPoints

import com.menome.util.ApplicationConfiguration
import com.menome.util.PreferenceType
import com.menome.util.RabbitMQ
import com.menome.util.messageBuilder.Connection
import com.menome.util.messageBuilder.MessageBuilder
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Produces
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Controller("/GenerateTestMessages")
class GenerateTestMessagesController {

    static Logger log = LoggerFactory.getLogger(GenerateTestMessagesController.class)

    @Get("/{messagesToCreate}")
    @Produces(MediaType.TEXT_HTML)
    String index(@PathVariable Integer messagesToCreate) {

        def rabbitChannel = RabbitMQ.openRabbitMQChannel(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), RabbitMQ.createRabbitConnectionFactory())
        def victoriaOffice = Connection.builder().Name("Menome Victoria").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).Properties(["SourceSystem": "menome_test_framework"]).build()
        def project = Connection.builder().Name("theLink").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).Properties(["SourceSystem": "menome_test_framework"]).build()
        def team = Connection.builder().Name("theLink Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).Properties(["SourceSystem": "menome_test_framework"]).build()
        def victoriaEmployee = MessageBuilder.builder()
                .Name("Konrad Aust")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("menome_test_framework")
                .ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .Connections([victoriaOffice, project, team])
                .build()
                .toJSON()


        log.info("Creating ${messagesToCreate} test messages")
        (1..messagesToCreate).each { it ->
            String message = victoriaEmployee.replaceAll("konrad.aust@menome.com", "konrad.aust${UUID.randomUUID()}@menome.com")
            rabbitChannel.basicPublish(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), null, message.getBytes())
        }
        log.info("Done")
        "<h3>Created $messagesToCreate messages and sent to ${ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE)}</h3>"
    }
}
