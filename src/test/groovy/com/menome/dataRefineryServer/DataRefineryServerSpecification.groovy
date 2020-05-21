package com.menome.dataRefineryServer

import com.menome.MessagingSpecification
import com.menome.datarefinery.DataRefineryServer
import com.menome.util.Neo4J
import spock.lang.IgnoreRest

import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

class DataRefineryServerSpecification extends MessagingSpecification {


    @IgnoreRest
    def "test 50 messages delivered over rabbit to server"() {
        given:
        DataRefineryServer.startServer()
        publishMessagesToRabbit(25000)
        expect:
        sleep(10000)
        //todo:Write some neo4j queries here
        1 == 1
        cleanup:
        1 == 1
        //DataRefineryServer.stopServer()
    }


    def "publish symend messages"() {
        given:
        //DataRefineryServer.startServer()
        def driver = Neo4J.openDriver()
        Neo4J.run(driver, "match (n) detach delete n")
        await().atMost(1, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 0 }

        log.info("Starting message building")
        List<String> messages = buildSymendMessages(20_000)
        log.info("Done message building")
        publishSymendMessagesToRabbit(messages)
        log.info("Done rabbit publishing")
        expect:
        1==1
        //await().atMost(5, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() >= 50_000 }

    }

    void publishMessagesToRabbit(int messageCount) {
        def rabbitConnectionFactory = createRabbitConnectionFactory()

        def rabbitChannel = openRabbitMQChanel(RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, rabbitConnectionFactory)
        (1..messageCount).each { it ->
            String message = victoriaEmployee.replaceAll("konrad.aust@menome.com", "konrad.aust$it@menome.com")
            //println "Publishing:$message"
            rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, message.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == messageCount }
    }

    void publishSymendMessagesToRabbit(List<String> messages) {
        def rabbitConnectionFactory = createRabbitConnectionFactory()

        def rabbitChannel = openRabbitMQChanel(RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, rabbitConnectionFactory)
        messages.each { message ->
            rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, message.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == messages.size() }
    }


}
