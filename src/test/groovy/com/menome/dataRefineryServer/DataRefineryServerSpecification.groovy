package com.menome.dataRefineryServer

import com.menome.MessagingSpecification
import com.menome.datarefinery.DataRefineryServer
import com.rabbitmq.client.impl.MicrometerMetricsCollector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry

import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

class DataRefineryServerSpecification extends MessagingSpecification {


    def "test 50 messages delivered over rabbit to server"() {
        given:
        DataRefineryServer.startServer()
        publishMessagesToRabbit(503)
        expect:
        sleep(10000)
        //todo:Write some neo4j queries here
        1 == 1
        cleanup:
        1==1
        //DataRefineryServer.stopServer()
    }

    void publishMessagesToRabbit(int messageCount) {
        def rabbitConnectionFactory = createRabbitConnectionFactory()
        def metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)

        def rabbitChannel = openRabbitMQChanel(RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY,rabbitConnectionFactory)
        (1..messageCount).each { it ->
            String message = victoriaEmployee.replaceAll("konrad.aust@menome.com", "konrad.aust$it@menome.com")
            //println "Publishing:$message"
            rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, message.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == messageCount }

    }

}
