package com.menome.rabbitIntegration

import com.menome.MessagingSpecification
import com.menome.messageProcessor.MessageProcessor
import com.menome.util.Neo4J
import com.rabbitmq.client.*
import com.rabbitmq.client.impl.MicrometerMetricsCollector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import spock.lang.Shared

import java.text.SimpleDateFormat
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

class RabbitMQVolumeSpecification extends MessagingSpecification {

    static Logger log = LoggerFactory.getLogger(RabbitMQVolumeSpecification.class)

    @Shared
    static ConnectionFactory rabbitConnectionFactory

    @Shared
    static GenericContainer rabbitMQContainer

    @Shared
    static MicrometerMetricsCollector metrics

    @Shared
    GenericContainer neo4JContainer

    @Shared
    Driver neo4JDriver


    protected static ConnectionFactory createRabbitConnectionFactory(GenericContainer rabbitMQContainer) {
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory()
        rabbitConnectionFactory.host = rabbitMQContainer.containerIpAddress
        rabbitConnectionFactory.port = rabbitMQContainer.getMappedPort(RABBITMQ_PORT)
        rabbitConnectionFactory.username = "menome"
        rabbitConnectionFactory.password = "menome"

        return rabbitConnectionFactory
    }

    protected static ConnectionFactory createRabbitConnectionFactory() {
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory()
        rabbitConnectionFactory.host = "127.0.0.1"
        rabbitConnectionFactory.port = RABBITMQ_PORT
        rabbitConnectionFactory.username = "menome"
        rabbitConnectionFactory.password = "menome"

        return rabbitConnectionFactory
    }

    protected static Channel openRabbitMQChanel(GenericContainer rabbitMQContainer, String queue, String exchange, String routingKey) {

        Connection rabbitConnection = rabbitConnectionFactory.newConnection()
        Channel rabbitChannel = rabbitConnection.createChannel()
        rabbitChannel.queueDeclare(queue, true, false, false, null)
        rabbitChannel.exchangeDeclare(exchange, "topic", true)
        rabbitChannel.queueBind(queue, exchange, routingKey)

        return rabbitChannel
    }

    def xsetupSpec() {
        //rabbitMQContainer = createAndStartRabbitMQContainer(Network.newNetwork())
        rabbitConnectionFactory = createRabbitConnectionFactory()
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)
        neo4JContainer = createAndStartNeo4JContainer(Network.newNetwork())
        neo4JDriver = Neo4J.openDriver(neo4JContainer)
    }

    def "write for server"() {
        given:
        rabbitConnectionFactory = createRabbitConnectionFactory()
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)

        def rabbitChannel = openRabbitMQChanel(null, RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY)
        when:
        (1..5000).each { it ->
            String message = messageWithConnections.replaceAll("konrad.aust@menome.com", "konrad.aust$it@menome.com")
            println "Publishing:$message"
            rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, message.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == 5000 }
        then:
        1 == 1
    }

    def "write 5000 messages to rabbit"() {
        given:
        def rabbitChannel = openRabbitMQChanel(rabbitMQContainer, RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY)
        when:
        (1..5000).each { it ->
            String message = messageWithConnections.replaceAll("konrad.aust@menome.com", "konrad.aust$it@menome.com")
            rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, message.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == 5000 }
        then:
        println(new SimpleDateFormat("yyyy/MM/dd HH:mm:SSSSS").format(new Date()))
        Instant start = Instant.now()
        rabbitChannel.basicConsume(RABBITMQ_QUEUE_NAME, new DefaultConsumer(rabbitChannel) {
            @Override
            void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag()
                MessageProcessor processor = new MessageProcessor()
                Session session = neo4JDriver.session()

                processor.process(new String(body))
                //println new String(body)
                List<String> statements = processor.getNeo4JStatements()
                Neo4J.executeStatementListInSession(statements, session)
                session.close()

                rabbitChannel.basicAck(deliveryTag, false)
            }
        })
        await().atMost(5, TimeUnit.MINUTES).until { metrics.consumedMessages.count() >= 5000 }
        Instant end = Instant.now()
        Duration duration = Duration.between(start, end)
        println(duration)
        println "Done..."
        //sleep(100000)
    }
}
