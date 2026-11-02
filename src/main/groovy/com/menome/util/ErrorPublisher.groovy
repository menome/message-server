package com.menome.util

import com.menome.messageBatchProcessor.MessageError
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Publishes messages that could not be applied to a durable error queue (default "<queue>.errors") so they can be
 * inspected and replayed instead of being lost. Never throws: if the error queue is unreachable the message is logged.
 */
class ErrorPublisher implements Closeable {

    static Logger log = LoggerFactory.getLogger(ErrorPublisher.class)

    private final ConnectionFactory connectionFactory
    private final String errorQueue
    private Connection connection
    private Channel channel

    ErrorPublisher(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory
        this.errorQueue = ApplicationConfiguration.errorQueue()
    }

    synchronized void publish(List<MessageError> errors) {
        errors.each { MessageError error ->
            try {
                Channel ch = channel()
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .deliveryMode(MessageProperties.PERSISTENT_BASIC.deliveryMode)
                        .contentType("application/json")
                        .headers(["x-error": truncate(error.errorText)] as Map<String, Object>)
                        .build()
                ch.basicPublish("", errorQueue, props, (error.message ?: "").getBytes("UTF-8"))
            } catch (Exception e) {
                log.error("Unable to publish to error queue {} ({}). Message: {} Error: {}", errorQueue, e.message, error.message, error.errorText)
                reset()
            }
        }
    }

    private Channel channel() {
        if (channel == null || !channel.isOpen()) {
            reset()
            connection = connectionFactory.newConnection("${ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE)}-errors")
            channel = connection.createChannel()
            channel.queueDeclare(errorQueue, true, false, false, null)
        }
        channel
    }

    private void reset() {
        try { channel?.close() } catch (Exception ignored) {}
        try { connection?.close() } catch (Exception ignored) {}
        channel = null
        connection = null
    }

    private static String truncate(String text) {
        // AMQP short strings are limited; keep headers small
        (text ?: "").take(1000)
    }

    @Override
    synchronized void close() {
        reset()
    }
}
