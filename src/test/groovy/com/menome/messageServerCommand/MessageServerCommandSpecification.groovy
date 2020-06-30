package com.menome.messageServerCommand

import com.menome.MessageServerCommand
import com.menome.SymendMessagingSpecification
import com.menome.util.ApplicationConfiguration
import com.menome.util.Neo4J
import com.menome.util.PreferenceType
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.impl.MicrometerMetricsCollector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Ignore
import spock.lang.Shared

import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

class MessageServerCommandSpecification extends SymendMessagingSpecification {

    static Logger log = LoggerFactory.getLogger(MessageServerCommandSpecification.class)

    @Shared
    static ConnectionFactory rabbitConnectionFactory

    @Shared
    static MicrometerMetricsCollector metrics

    @Shared
    Driver driver

    def setupSpec() {
        rabbitConnectionFactory = createRabbitConnectionFactory()
        driver = Neo4J.openDriver()
    }

    def setup() {
        Neo4J.deleteAllTestNodes()
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)
    }


    def "process 5000 employee messages in less than 15 seconds"() {
        given:
        def messagesToWrite = 5_000
        writeEmployeeMessages(messagesToWrite)

        when:
        MessageServerCommand.main()
        await().atMost(30, TimeUnit.SECONDS).until { MessageServerCommand.getTotalMessagesProcessedByServer() == messagesToWrite }

        then:
        messagesToWrite == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()

        cleanup:
        MessageServerCommand.shutdown()
    }

    def "process 50,000 employee messages in less than 60 seconds"() {
        given:
        def messagesToWrite = 50_000
        writeEmployeeMessages(messagesToWrite)

        when:
        MessageServerCommand.main()
        await().atMost(60, TimeUnit.SECONDS).until { MessageServerCommand.getTotalMessagesProcessedByServer() == messagesToWrite }
        // This extra await is here to allow Neo4J to finish writing the transaction
        await().atMost(60, TimeUnit.SECONDS).until { Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt() == messagesToWrite }

        then:
        messagesToWrite == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()

        cleanup:
        MessageServerCommand.shutdown()
    }

    def "process 500,000 with two message server instances in less thank five minutes"() {
        given:
        def messagesToWrite = 500_000
        writeEmployeeMessages(messagesToWrite)
        when:
        MessageServerCommand.main()
        System.setProperty(PreferenceType.HTTP_SERVER_PORT.name(), "-1")
        MessageServerCommand.main()

        await().atMost(5, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt() == messagesToWrite }
        then:
        messagesToWrite == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()

        cleanup:
        MessageServerCommand.shutdown()

    }

    @Ignore
    def "process 200,000 Symend with two message server instances"() {
        given:
        def messagesToWrite = 200_000
        writeSymendMessages(messagesToWrite)
        when:
        MessageServerCommand.main()
        System.setProperty(PreferenceType.HTTP_SERVER_PORT.name(), "-1")
        MessageServerCommand.main()

        await().atMost(5, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (e:CollectionEvent) return count(e) as count").single().get("count").asInt() == messagesToWrite }
        then:
        messagesToWrite == Neo4J.run(driver, "match (e:CollectionEvent) return count(e) as count").single().get("count").asInt()

        cleanup:
        MessageServerCommand.shutdown()

    }


    private static writeEmployeeMessages(messagesToWrite) {
        def rabbitChannel = openRabbitMQChanel(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), rabbitConnectionFactory)

        (1..messagesToWrite).each {
            String message = victoriaEmployee.replaceAll("konrad.aust@menome.com", "konrad.aust${UUID.randomUUID()}@menome.com")
            rabbitChannel.basicPublish(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), null, message.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == messagesToWrite }
    }

    def writeSymendMessages(Integer messagesToWrite) {
        def rabbitChannel = openRabbitMQChanel(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE),ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE),ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), rabbitConnectionFactory)

        def loopCounter = messagesToWrite / 50_000
        (1..loopCounter).each {
            def messages = buildSymendMessages(50_000, Integer.MAX_VALUE, Integer.MAX_VALUE)
            messages.each { message ->
                rabbitChannel.basicPublish(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), null, message.getBytes())
            }
        }
        await().atMost(2, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == messagesToWrite}

    }

}

