package com.menome.util;

import com.rabbitmq.client.ConnectionFactory;

class RabbitMQ {

    private static final int RABBITMQ_PORT = 5672

    static ConnectionFactory createRabbitConnectionFactory() {
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory()
        rabbitConnectionFactory.setHost("127.0.0.1")
        rabbitConnectionFactory.setPort(RABBITMQ_PORT)
        rabbitConnectionFactory.setUsername("menome")
        rabbitConnectionFactory.setPassword("menome")

        rabbitConnectionFactory
    }
}
