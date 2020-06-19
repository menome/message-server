package com.menome

import com.menome.messageBatchProcessor.MessageBatchProcessor
import com.menome.messageBatchProcessor.MessageBatchResult
import com.menome.messageBatchProcessor.MessageBatchSummary
import com.menome.util.*
import com.rabbitmq.client.Address
import com.rabbitmq.client.ConnectionFactory
import io.micronaut.configuration.picocli.PicocliRunner
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.Micronaut
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import picocli.CommandLine
import picocli.CommandLine.Command
import reactor.core.publisher.Mono
import reactor.rabbitmq.RabbitFlux
import reactor.rabbitmq.ReceiverOptions

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

@Command(name = 'messageServer'
        , description = '...'
        , mixinStandardHelpOptions = true
        , header = [
                " __  __",
                "|  \\/  | ___ _ __   ___  _ __ ___   ___",
                "| |\\/| |/ _ \\ '_ \\ / _ \\| '_ ` _ \\ / _ \\",
                "| |  | |  __/ | | | (_) | | | | | |  __/",
                "|_|  |_|\\___|_| |_|\\___/|_| |_| |_|\\___|"
        ])
class MessageServerCommand implements Runnable {

    static Logger log = LoggerFactory.getLogger(MessageServerCommand.class)
    static AtomicLong totalMessagesProcessedByServer = new AtomicLong(0)
    static AtomicLong totalErrorsProcessedByServer = new AtomicLong(0)
    static ApplicationContext applicationContext = null

    static void main(String[] args) throws Exception {
        PicocliRunner.run(MessageServerCommand.class, args)
    }

    void run() {
        startServer()
    }

    static void shutdown(){
        if (applicationContext){
            applicationContext.close()
        }
    }

    static void startServer() {
        displayBanner()
        log.info("Starting Server")
        // Start the http server for monitoring, health, etc.
        log.info("Starting Monitoring Services")

        System.setProperty("micronaut.server.port",ApplicationConfiguration.getString(PreferenceType.HTTP_SERVER_PORT))
        applicationContext = Micronaut.run(MessageServerCommand.class)

        ConnectionFactory rabbitConnectionFactory = connectToRabbitMQ()
        Driver driver = connectToNeo4J()
        connectToRedis()

        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(rabbitConnectionFactory)
                .connectionSupplier({ cf -> cf.newConnection([new Address(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_HOST),ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_PORT))], ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE)) })


        log.info("Message Server waiting for messages on queue {} processing messages with a batch size of {}", ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE), ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_BATCHSZIE))
        // This code connects to the Rabbit Server and waits for messages. Messages will be batched up and passed to the Message Processor. The results of the batch are then logged
        RabbitFlux.createReceiver(receiverOptions).consumeAutoAck(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE))
                .map({ rabbitMsg -> new String(rabbitMsg.getBody()) })
                .bufferTimeout(ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_BATCHSZIE), Duration.ofSeconds(2))
                .map({ messages -> MessageBatchProcessor.process(messages, driver) })
                .map({ messageBatchResult -> logBatchResult(messageBatchResult) })
                .map({ messageBatchResult -> updateServerStats(messageBatchResult) })
                .subscribe()

        log.info("Server Started")

    }

    static void displayBanner() {
        String[] banner = new CommandLine(new MessageServerCommand()).commandSpec.usageMessage().header()
        banner.each() { line ->
            log.info(CommandLine.Help.Ansi.AUTO.string(line))
        }
    }

    private static ConnectionFactory connectToRabbitMQ() {
        ConnectionFactory rabbitConnectionFactory = RabbitMQ.createRabbitConnectionFactory()
        if (RabbitMQ.connectionOk(rabbitConnectionFactory)) {
            log.info("Connected to Rabbit MQ Server OK")
        } else {
            log.error("Unable to connect to Rabbit MQ Server")
            System.exit(-1)
        }
        rabbitConnectionFactory
    }

    static Driver connectToNeo4J() {
        Driver driver = null
        try {
            driver = Neo4J.openDriver()
            def result = Neo4J.run(driver, "call dbms.components() yield name, versions, edition")
            def record = result.single()
            def version = record.get("versions").values()[0].asString()
            def edition = record.get("edition").asString()
            log.info("Connected to Neo4J Database Server OK - version:{} edition:{}", version, edition)
        } catch (Exception ignored) {
            log.error("Unable to connect to Neo4J Database Server")
            System.exit(-1)
        }
        driver
    }

    static void connectToRedis() {
        if (Redis.connectionOk()) {
            log.info("Connected to Redis cache server OK")
        } else {
            log.info("Unable to connect to Redis cache. Connection node caching is disabled")
            System.setProperty(PreferenceType.USE_REDIS_CACHE.name(), "N")
        }
    }


    private static MessageBatchResult logBatchResult(MessageBatchResult result) {
        MessageBatchSummary summary = result.getBatchSummary()
        log.info("Processed {} messages with {} errors in {} ms rate {} messages/s", summary.getSuccessCount(), summary.getErrorCount(), summary.getBatchProcessingDuration().toMillis(), summary.getRate())

        if (result.getErrors().size() > 0) {
            result.getErrors().forEach({ msg -> log.error(msg.getErrorText()) })
            //todo: republish the error message to an Error queue on the bus
        }

        return result
    }

    private static Mono<MessageBatchResult> updateServerStats(MessageBatchResult result) {
        MessageBatchSummary summary = result.getBatchSummary()
        totalMessagesProcessedByServer.getAndAdd(summary.successCount)
        totalErrorsProcessedByServer.getAndAdd(summary.errorCount)
        return Mono.just(result)
    }

    static Long getTotalMessagesProcessedByServer() {
        totalMessagesProcessedByServer.getPlain()
    }

    static Long getTotalErrorsProcessedByServer() {
        totalErrorsProcessedByServer.getPlain()
    }
}
