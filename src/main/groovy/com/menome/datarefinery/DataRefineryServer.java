package com.menome.datarefinery;

import com.menome.messageBatchProcessor.MessageBatchProcessor;
import com.menome.messageBatchProcessor.MessageBatchResult;
import com.menome.messageBatchProcessor.MessageBatchSummary;
import com.menome.util.Neo4J;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.ReceiverOptions;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
                .map(messages -> MessageBatchProcessor.process(messages, driver))
                .map(DataRefineryServer::logBatchResult)
                .subscribe();

        log.info("Server Started");

    }

    private static Mono<MessageBatchResult> logBatchResult(MessageBatchResult result) {
        MessageBatchSummary batchSummary = result.getBatchSummary();
        BigDecimal millis = new BigDecimal(batchSummary.getBatchProcessingDuration().toMillis());
        BigDecimal successCount = new BigDecimal(batchSummary.getSuccessCount());
        BigDecimal errorCount = new BigDecimal(batchSummary.getErrorCount());
        if (millis.compareTo(BigDecimal.ZERO) == 0) {
            millis = BigDecimal.ONE;
        }
        BigDecimal rate = successCount.divide(millis, 3,RoundingMode.HALF_EVEN).multiply(BigDecimal.valueOf(1000));
        rate = rate.setScale(0,RoundingMode.HALF_EVEN);
        log.info("Processed {} messages with {} errors in {} ms rate {} messages/s", successCount, errorCount, millis, rate);

        if (result.getErrors().size() > 0) {
            result.getErrors().forEach(msg -> log.error(msg.getErrorText()));
            //todo: republish the error message to an Error queue on the bus
        }

        return Mono.just(result);
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