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
        rabbitConnectionFactory.automaticRecoveryEnabled = true

        if (ApplicationConfiguration.getString(PreferenceType.SHOW_CONNECTION_LOG_OUTPUT) == "Y") {
            log.info("Connecting to RabbitMQ server {} on port {} with user {}", host, port, username)
        }

        rabbitConnectionFactory
    }

    /** The caller owns the channel and its connection (channel.connection) and must close both. */
    static Channel openRabbitMQChannel(String queue, String exchange, String routingKey, ConnectionFactory rabbitConnectionFactory) {
        Connection rabbitConnection = rabbitConnectionFactory.newConnection()
        try {
            Channel rabbitChannel = rabbitConnection.createChannel()
            declareTopology(rabbitChannel, queue, exchange, routingKey)
            rabbitChannel
        } catch (Exception e) {
            rabbitConnection.close()
            throw e
        }
    }

    private static void declareTopology(Channel channel, String queue, String exchange, String routingKey) {
        channel.queueDeclare(queue, true, false, false, null)
        channel.exchangeDeclare(exchange, ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE_TYPE), true)
        channel.queueBind(queue, exchange, routingKey)
    }

    static boolean connectionOk(ConnectionFactory connectionFactory) {
        try {
            connectionFactory.newConnection().withCloseable { Connection connection ->
                connection.createChannel().withCloseable { Channel channel ->
                    declareTopology(channel, ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE),
                            ApplicationConfiguration.getString(PreferenceType.RABBITMQ_EXCHANGE), "")
                }
            }
            true
        } catch (Exception ignored) {
            false
        }
    }
}
