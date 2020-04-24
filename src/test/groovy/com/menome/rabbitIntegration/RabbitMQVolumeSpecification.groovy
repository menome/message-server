package com.menome.rabbitIntegration

import com.menome.MessagingSpecification
import com.rabbitmq.client.*
import com.rabbitmq.client.impl.MicrometerMetricsCollector
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import spock.lang.Shared

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


    protected static ConnectionFactory createRabbitConnectionFactory(GenericContainer rabbitMQContainer){
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory()
        rabbitConnectionFactory.host = rabbitMQContainer.containerIpAddress
        rabbitConnectionFactory.port = rabbitMQContainer.getMappedPort(RABBITMQ_PORT)
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

    def setupSpec(){
        rabbitMQContainer = createAndStartRabbitMQContainer(Network.newNetwork())
        rabbitConnectionFactory = createRabbitConnectionFactory(rabbitMQContainer)
        metrics = new MicrometerMetricsCollector(new SimpleMeterRegistry())
        rabbitConnectionFactory.setMetricsCollector(metrics)
    }

    def "write 5000 messages to rabbit"() {
        given:
        def rabbitChannel = openRabbitMQChanel(rabbitMQContainer, RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY)
        when:
        (1..5000).each { it->
            String msg = "Some Message $it"
            rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, msg.getBytes())
        }
        await().atMost(5, TimeUnit.MINUTES).until { metrics.publishedMessages.count() == 5000 }
        then:
        1 == 1
        rabbitChannel.basicConsume(RABBITMQ_QUEUE_NAME, new DefaultConsumer(rabbitChannel) {
            @Override
            void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag();
                String foo = new String(body)
                println foo
                rabbitChannel.basicAck(deliveryTag, false);
            }
        })
        await().atMost(5, TimeUnit.MINUTES).until { metrics.consumedMessages.count() == 5000 }
    }
}
