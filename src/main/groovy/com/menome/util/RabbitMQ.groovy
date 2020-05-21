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
        def host = getHost()
        def port = Optional.ofNullable(System.getenv("RABBITMQ_PORT")).orElse("5672").toInteger()
        def username = Optional.ofNullable(System.getenv("RABBITMQ_USER")).orElse("menome")
        def password = Optional.ofNullable(System.getenv("RABBITMQ_PASSWORD")).orElse("menome")
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
            rabbitChannel.queueBind(getQueue(),getExchange(),"")
        } catch (Exception e) {
            ok = Boolean.FALSE
        }
        ok
    }

    static String getQueue(){
        Optional.ofNullable(System.getenv("RABBITMQ_QUEUE")).orElse("test_queue")
    }

    static String getHost() {
        Optional.ofNullable(System.getenv("RABBITMQ_HOST")).orElse("localhost")
    }

    static Integer getBatchSize() {
        Optional.ofNullable(System.getenv("RABBITMQ_BATCHSZIE")).orElse("5000").toInteger()
    }

    //todo: It would be nice to get rid of the exchange declaration. The rabbit-reactor library seems to have a way of connecting to the queue without the exchange
    static String getExchange() {
        Optional.ofNullable(System.getenv("RABBITMQ_EXCHANGE")).orElse("test_exchange")
        null
    }
}
