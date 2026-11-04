package com.menome.messageServerEndPoints

import com.menome.util.ApplicationConfiguration
import com.menome.util.PreferenceType
import com.menome.util.RabbitMQ
import com.menome.util.messageBuilder.Connection
import com.menome.util.messageBuilder.MessageBuilder
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Produces
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/** Test tooling. Disabled unless ENABLE_TEST_ENDPOINTS=Y. */
@Controller("/GenerateTestMessages")
class GenerateTestMessagesController {

    static final int MAX_MESSAGES = 1_000_000

    static Logger log = LoggerFactory.getLogger(GenerateTestMessagesController.class)

    @Post("/{messagesToCreate}")
    @Produces(MediaType.TEXT_HTML)
    HttpResponse<String> index(@PathVariable Integer messagesToCreate) {
        if (!ApplicationConfiguration.getBoolean(PreferenceType.ENABLE_TEST_ENDPOINTS)) {
            return HttpResponse.notFound()
        }
        if (messagesToCreate < 1 || messagesToCreate > MAX_MESSAGES) {
            return HttpResponse.<String> status(HttpStatus.BAD_REQUEST).body("messagesToCreate must be between 1 and ${MAX_MESSAGES}")
        }

        String exchange = ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE)
        String route = ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE)
        def channel = RabbitMQ.openRabbitMQChannel(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE), exchange, route, RabbitMQ.createRabbitConnectionFactory())
        try {
            def victoriaOffice = Connection.builder().Name("Victoria Office").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).Properties(["SourceSystem": "test_framework"]).build()
            def project = Connection.builder().Name("Apollo").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).Properties(["SourceSystem": "test_framework"]).build()
            def team = Connection.builder().Name("Apollo Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).Properties(["SourceSystem": "test_framework"]).build()
            def victoriaEmployee = MessageBuilder.builder()
                    .Name("Konrad Aust")
                    .NodeType("Employee")
                    .Priority(1)
                    .SourceSystem("test_framework")
                    .ConformedDimensions("Email": "konrad.aust@example.com", "EmployeeId": 12345)
                    .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                    .Connections([victoriaOffice, project, team])
                    .build()
                    .toJSON()

            log.info("Creating ${messagesToCreate} test messages")
            (1..messagesToCreate).each {
                String message = victoriaEmployee.replaceAll("konrad.aust@example.com", "konrad.aust${UUID.randomUUID()}@example.com")
                channel.basicPublish(exchange, route, null, message.getBytes())
            }
            log.info("Done")
        } finally {
            def connection = channel.connection
            try { channel.close() } catch (Exception ignored) {}
            connection.close()
        }
        HttpResponse.ok("<h3>Created $messagesToCreate messages and sent to ${ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE)}</h3>")
    }
}
