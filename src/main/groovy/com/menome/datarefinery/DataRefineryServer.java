package com.menome.datarefinery;

import com.menome.messageBatchProcessor.MessageBatchProcessor;
import com.menome.util.Neo4J;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.ReceiverOptions;

import java.time.Duration;

public class DataRefineryServer {

    private static final String RABBITMQ_QUEUE_NAME = "test_queue";
    private static final int RABBITMQ_PORT = 5672;

    static Logger log = LoggerFactory.getLogger(DataRefineryServer.class);

    public static void main(String[] args) {
        startServer();
    }

    public static void startServer() {
        log.info("Starting Server...");

        ConnectionFactory rabbitConnectionFactory = createRabbitConnectionFactory();

        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(rabbitConnectionFactory)
                .connectionSupplier(cf -> cf.newConnection(new Address[]{new Address("127.0.0.1")}, RABBITMQ_QUEUE_NAME));

        Driver driver = Neo4J.openDriver();

        int BATCH_SIZE = 5_000;
        RabbitFlux.createReceiver(receiverOptions).consumeAutoAck(RABBITMQ_QUEUE_NAME)
                .map(rabbitMsg -> new String(rabbitMsg.getBody()))
                .bufferTimeout(BATCH_SIZE, Duration.ofSeconds(2))
                .map(messages->MessageBatchProcessor.process(messages,driver,false))
                .subscribe();

        log.info("Server Started");

    }

    //todo: Figure out where to read the connection information from
    private static ConnectionFactory createRabbitConnectionFactory() {
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
        rabbitConnectionFactory.setHost("127.0.0.1");
        rabbitConnectionFactory.setPort(RABBITMQ_PORT);
        rabbitConnectionFactory.setUsername("menome");
        rabbitConnectionFactory.setPassword("menome");

        return rabbitConnectionFactory;
    }

}