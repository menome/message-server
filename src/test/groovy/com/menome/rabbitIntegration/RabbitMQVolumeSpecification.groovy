package com.menome.rabbitIntegration

import com.menome.MessagingSpecification
import com.menome.messageProcessor.MessageProcessor
import com.menome.util.Neo4J
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import com.rabbitmq.client.impl.MicrometerMetricsCollector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
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


    def setupSpec() {
        rabbitConnectionFactory = createRabbitConnectionFactory()
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)
    }

    def "write for server"() {
        given:
        rabbitConnectionFactory = createRabbitConnectionFactory()
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)

        def rabbitChannel = openRabbitMQChanel(RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY,rabbitConnectionFactory)
        def messagesToWrite = 20_000
        when:
        (1..messagesToWrite).each { it ->
            String message = messageWithConnections.replaceAll("konrad.aust@menome.com", "konrad.aust$it@menome.com")
            //println "Publishing:$message"
            rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, message.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == messagesToWrite }
        then:
        1 == 1
        println("Done...")
    }

    def "write 5000 messages to rabbit"() {
        given:
        def rabbitChannel = openRabbitMQChanel(RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY,rabbitConnectionFactory)
        when:


        Neo4J.run(neo4JDriver, "CREATE INDEX ON :Employee(Email,EmployeeId)")
        Neo4J.run(neo4JDriver, "CREATE INDEX ON :Card(Email,EmployeeId)")
        Neo4J.run(neo4JDriver, "MERGE (team:Card:Team {Code: 1337}) ON CREATE SET team.Uuid = apoc.create.uuid(),team.TheLinkAddedDate = datetime(), team.Name = \"theLink Product Team\" , team.PendingMerge = true")
        Neo4J.run(neo4JDriver, "MERGE (project:Card:Project {Code: 5}) ON CREATE SET project.Uuid = apoc.create.uuid(),project.TheLinkAddedDate = datetime(), project.Name = \"theLink\" , project.PendingMerge = true")
        Neo4J.run(neo4JDriver, "MERGE (office:Card:Office {City: \"Victoria\"}) ON CREATE SET office.Uuid = apoc.create.uuid(),office.TheLinkAddedDate = datetime(), office.Name = \"Menome Victoria\" , office.PendingMerge = true")

        //(1..5000).each { it ->
        int messagesToCreate = 50
        println("Creating $messagesToCreate")
        (1..messagesToCreate).each { it ->
            String message = messageWithConnections.replaceAll("konrad.aust@menome.com", "konrad.aust$it@menome.com")
            rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, message.getBytes())
        }

        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() >= messagesToCreate }
        then:
        println(new SimpleDateFormat("yyyy/MM/dd HH:mm:SSSSS").format(new Date()))
        Instant start = Instant.now()
        rabbitChannel.basicConsume(RABBITMQ_QUEUE_NAME, new DefaultConsumer(rabbitChannel) {
            @Override
            void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag()
                MessageProcessor processor = new MessageProcessor()
                Session session = neo4JDriver.session()
                List<String> statements = processor.process(new String(body))
                Neo4J.executeStatementListInSession(statements, session)
                session.close()
                rabbitChannel.basicAck(deliveryTag, false)
            }
        })
        await().atMost(5, TimeUnit.MINUTES).until { metrics.consumedMessages.count() >= 50 }
        Instant end = Instant.now()
        Duration duration = Duration.between(start, end)
        println(duration)
        println "Done..."
        sleep(1000000000)
    }
}
