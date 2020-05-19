package com.menome.datarefinery;

import com.menome.messageBatchProcessor.MessageBatchProcessor;
import com.menome.messageBatchProcessor.MessageBatchResult;
import com.menome.messageBatchProcessor.MessageBatchSummary;
import com.menome.util.Neo4J;
import com.menome.util.RabbitMQ;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.ReceiverOptions;

import java.time.Duration;

public class DataRefineryServer {

    private static final String RABBITMQ_QUEUE_NAME = "test_queue";


    static Logger log = LoggerFactory.getLogger(DataRefineryServer.class);

    public static void main(String[] args) {
        startServer();
    }

    public static void startServer() {
        log.info("Starting Server...");

        ConnectionFactory rabbitConnectionFactory = RabbitMQ.createRabbitConnectionFactory();

        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(rabbitConnectionFactory)
                .connectionSupplier(cf -> cf.newConnection(new Address[]{new Address("127.0.0.1")}, RABBITMQ_QUEUE_NAME));

        Driver driver = Neo4J.openDriver();

        int BATCH_SIZE = 5000;
        RabbitFlux.createReceiver(receiverOptions).consumeAutoAck(RABBITMQ_QUEUE_NAME)
                .map(rabbitMsg -> new String(rabbitMsg.getBody()))
                .bufferTimeout(BATCH_SIZE, Duration.ofSeconds(2))
                .map(messages -> MessageBatchProcessor.process(messages, driver))
                .map(DataRefineryServer::logBatchResult)
                .subscribe();

        log.info("Server Started");

    }

    private static Mono<MessageBatchResult> logBatchResult(MessageBatchResult result) {
        MessageBatchSummary summary = result.getBatchSummary();
        log.info("Processed {} messages with {} errors in {} ms rate {} messages/s", summary.getSuccessCount(), summary.getErrorCount(), summary.getBatchProcessingDuration().toMillis(), summary.getRate());

        if (result.getErrors().size() > 0) {
            result.getErrors().forEach(msg -> log.error(msg.getErrorText()));
            //todo: republish the error message to an Error queue on the bus
        }

        return Mono.just(result);
    }



}