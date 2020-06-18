package com.menome.rabbitIntegration

import com.menome.SymendMessagingSpecification
import com.menome.util.ApplicationConfiguration
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

class RabbitMQVolumeSpecification extends SymendMessagingSpecification {

    static Logger log = LoggerFactory.getLogger(RabbitMQVolumeSpecification.class)

    @Shared
    static ConnectionFactory rabbitConnectionFactory

    @Shared
    static MicrometerMetricsCollector metrics

    @Shared
    Driver neo4JDriver


    def setupSpec() {
        rabbitConnectionFactory = createRabbitConnectionFactory()
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)
    }

    @Ignore
    def "write 50,000 Employee Messages to rabbit"() {
        given:
        rabbitConnectionFactory = createRabbitConnectionFactory()
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)

        def rabbitChannel = openRabbitMQChanel(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE),ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE),ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), rabbitConnectionFactory)
        def messagesToWrite = 50_000
        when:
        (1..messagesToWrite).each { it ->
            String message = victoriaEmployee.replaceAll("konrad.aust@menome.com", "konrad.aust${UUID.randomUUID()}@menome.com")
            rabbitChannel.basicPublish(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), null, message.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == messagesToWrite }
        then:
        1 == 1
    }


    @Ignore
    def "write 250,000 Symend messages to rabbit"() {
        given:
        rabbitConnectionFactory = createRabbitConnectionFactory()
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)

        def rabbitChannel = openRabbitMQChanel(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE),ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE),ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), rabbitConnectionFactory)
        def messagesToWrite = 50_000

        when:
        (1..5).each {
            def messages = buildSymendMessages(messagesToWrite, Integer.MAX_VALUE, Integer.MAX_VALUE)
            messages.each { message ->
                rabbitChannel.basicPublish(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE), ApplicationConfiguration.getString(PreferenceType.RABBITMQ_ROUTE), null, message.getBytes())
            }
        }
        await().atMost(2, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == 250_000 }
        then:
        1 == 1

    }
}
