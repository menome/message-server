package com.menome.util


import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class RabbitMQ {


    static Logger log = LoggerFactory.getLogger(RabbitMQ.class)

    static ConnectionFactory createRabbitConnectionFactory() {
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory()
        def host = ApplicationConfiguration.getString(PreferenceType.RABBITMQ_HOST)
        def port = ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_PORT)
        def username = ApplicationConfiguration.getString(PreferenceType.RABBITMQ_USER)
        def password = ApplicationConfiguration.getString(PreferenceType.RABBITMQ_PASSWORD)
        rabbitConnectionFactory.host = host
        rabbitConnectionFactory.port = port
        rabbitConnectionFactory.username = username
        rabbitConnectionFactory.password = password

        log.info("Connecting to RabbitMQ server {} on port {} with user {}", host, port, username)

        rabbitConnectionFactory
    }

    static Channel openRabbitMQChannel(String queue, String exchange, String routingKey, ConnectionFactory rabbitConnectionFactory) {

        Connection rabbitConnection = rabbitConnectionFactory.newConnection()
        Channel rabbitChannel = rabbitConnection.createChannel()
        rabbitChannel.queueDeclare(queue, true, false, false, null)
        rabbitChannel.exchangeDeclare(exchange, "topic", true)
        rabbitChannel.queueBind(queue, exchange, routingKey)
        rabbitChannel
    }

    static boolean connectionOk(ConnectionFactory connectionFactory) {
        boolean ok = Boolean.TRUE
        try {
            def rabbitConnection = connectionFactory.newConnection()
            Channel rabbitChannel = rabbitConnection.createChannel()
            rabbitChannel.queueBind(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE),ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE),"")
        } catch (Exception ignored) {
            ok = Boolean.FALSE
        }
        ok
    }
}
