package com.menome

import com.menome.messageBatchProcessor.BatchPipeline
import com.menome.messageBatchProcessor.MessageBatchResult
import com.menome.messageBatchProcessor.MessageBatchSummary
import com.menome.messageBatchProcessor.StageTimings
import com.menome.util.*
import com.rabbitmq.client.Address
import com.rabbitmq.client.ConnectionFactory
import io.micrometer.core.instrument.MeterRegistry
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.rabbitmq.AcknowledgableDelivery
import reactor.rabbitmq.ConsumeOptions
import reactor.rabbitmq.Receiver
import reactor.util.retry.Retry
import java.nio.charset.StandardCharsets
import java.util.function.BiConsumer
import java.util.function.Function
import io.micronaut.configuration.picocli.PicocliRunner
import io.micronaut.context.ApplicationContext
import io.micronaut.runtime.Micronaut
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import picocli.CommandLine
import picocli.CommandLine.Command
import reactor.rabbitmq.RabbitFlux
import reactor.rabbitmq.ReceiverOptions

import java.time.Duration
import java.util.concurrent.TimeUnit
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
    static Driver neo4jDriver = null
    static ErrorPublisher errorPublisher = null
    static Receiver receiver = null
    static Disposable subscription = null


    static void main(String[] args) throws Exception {
        PicocliRunner.run(MessageServerCommand.class, args)
    }

    void run() {
        startServer()
    }

    static void shutdown() {
        subscription?.dispose()
        receiver?.close()
        errorPublisher?.close()
        neo4jDriver?.close()
        applicationContext?.close()
        subscription = null
        receiver = null
        errorPublisher = null
        neo4jDriver = null
        applicationContext = null
    }

    static void startServer() {
        displayBanner()
        log.info("Starting Server")
        // Start the http server for monitoring, health, etc.
        log.info("Starting Monitoring Services")

        System.setProperty("micronaut.server.port", ApplicationConfiguration.getString(PreferenceType.HTTP_SERVER_PORT))
        // application.yml ships with micronaut.metrics.enabled=false, which leaves no MeterRegistry bean to look up below
        System.setProperty("micronaut.metrics.enabled", ApplicationConfiguration.getBoolean(PreferenceType.ENABLE_METRICS).toString())
        applicationContext = Micronaut.run(MessageServerCommand.class)

        MeterRegistry meterRegistry = null
        if (ApplicationConfiguration.getBoolean(PreferenceType.ENABLE_METRICS)) {
            log.info("Metrics are enabled")
            meterRegistry = applicationContext.getBean(MeterRegistry)
        } else {
            log.info("Metrics are not enabled")
        }

        ConnectionFactory rabbitConnectionFactory = connectToRabbitMQ()
        neo4jDriver = connectToNeo4J()
        errorPublisher = new ErrorPublisher(rabbitConnectionFactory)

        String queue = ApplicationConfiguration.getString(PreferenceType.RABBITMQ_QUEUE)
        int batchSize = ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_BATCH_SIZE)
        Driver driver = neo4jDriver
        ErrorPublisher publisher = errorPublisher
        MeterRegistry registry = meterRegistry

        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(rabbitConnectionFactory)
                .connectionSupplier({ cf -> cf.newConnection([new Address(ApplicationConfiguration.getString(PreferenceType.RABBITMQ_HOST), ApplicationConfiguration.getInteger(PreferenceType.RABBITMQ_PORT))], queue) })
        receiver = RabbitFlux.createReceiver(receiverOptions)

        int writers = Math.max(1, ApplicationConfiguration.getInteger(PreferenceType.WRITER_THREADS))
        int parsers = Math.max(1, ApplicationConfiguration.getInteger(PreferenceType.PARSER_THREADS))
        // Unacknowledged messages RabbitMQ may hand out: the batches being parsed or written, the one waiting in between,
        // and the one being filled. Fewer would starve the pipeline, more would only read ahead.
        int prefetch = batchSize * (parsers + writers + 1)

        log.info("Message Server waiting for messages on queue {} processing messages with a batch size of {}, {} parser and {} writer thread(s)", queue, batchSize, parsers, writers)
        // Messages are acknowledged only after the batch is committed to Neo4J, so a crash or outage redelivers them
        // (all writes are idempotent MERGEs). Messages that cannot be applied are moved to the error queue.
        Flux<BatchPipeline.Batch<List<AcknowledgableDelivery>>> batches = Flux.defer({ receiver.consumeManualAck(queue, new ConsumeOptions().qos(prefetch)) })
                .bufferTimeout(batchSize, Duration.ofSeconds(2), true)
                .map({ List<AcknowledgableDelivery> deliveries ->
                    new BatchPipeline.Batch<List<AcknowledgableDelivery>>(deliveries, deliveries.collect { new String(it.getBody(), StandardCharsets.UTF_8) })
                } as Function)
        subscription = BatchPipeline.run(batches, driver, registry, parsers, writers,
                { List<AcknowledgableDelivery> deliveries, MessageBatchResult result -> completeBatch(deliveries, result, registry, publisher) } as BiConsumer,
                { List<AcknowledgableDelivery> deliveries, Throwable error ->
                    // Infrastructure failure (not bad data): put the messages back; the pipeline then backs off
                    log.error("Batch of {} messages could not be processed, requeueing: {}", deliveries.size(), error.toString())
                    deliveries.each { it.nack(true) }
                } as BiConsumer, Duration.ofSeconds(5))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry({ signal -> log.error("Message stream failed, resubscribing: {}", signal.failure().toString()) }))
                .subscribe({ BatchPipeline.Processed processed -> updateServerStats(processed.result) }, { error -> log.error("Message stream terminated", error) })

        log.info("Server Started")
    }

    /** After a batch is committed: report the bad messages, then acknowledge the batch. Failing here requeues the batch. */
    private static void completeBatch(List<AcknowledgableDelivery> deliveries, MessageBatchResult result, MeterRegistry registry, ErrorPublisher publisher) {
        logBatchResult(result)
        publisher.publish(result.getErrors())
        long ackStart = System.nanoTime()
        deliveries.each { it.ack() }
        registry?.timer("messageserver.stage.duration", "stage", StageTimings.ACK)?.record(Duration.ofNanos(System.nanoTime() - ackStart))
    }

    static void displayBanner() {
        String[] banner = new CommandLine(new MessageServerCommand()).commandSpec.usageMessage().header()
        banner.each() { line ->
            log.info(CommandLine.Help.Ansi.AUTO.string(line))
        }
    }

    private static ConnectionFactory connectToRabbitMQ() {
        ConnectionFactory rabbitConnectionFactory = RabbitMQ.createRabbitConnectionFactory()
        if (waitUntil(Duration.ofMinutes(1)) { RabbitMQ.connectionOk(rabbitConnectionFactory) }) {
            log.info("Connected to Rabbit MQ Server OK")
        } else {
            log.error("Unable to connect to Rabbit MQ Server")
            System.exit(-1)
        }

        rabbitConnectionFactory
    }

    static Driver connectToNeo4J() {
        if (!waitUntil(Duration.ofMinutes(1)) { Neo4J.connectionOk() }) {
            log.error("Unable to connect to Neo4J Database Server")
            System.exit(-1)
        }
        Driver driver = Neo4J.openDriver()
        log.info("Connected to Neo4J Database Server OK - version:{} edition:{}", Neo4J.version(), Neo4J.edition())
        driver
    }

    private static boolean waitUntil(Duration timeout, Closure<Boolean> condition) {
        long deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            if (condition()) return true
            Thread.sleep(500)
        }
        false
    }

    private static MessageBatchResult logBatchResult(MessageBatchResult result) {
        MessageBatchSummary summary = result.getBatchSummary()
        log.info("Processed {} messages with {} errors in {} ms rate {} messages/s", summary.getSuccessCount(), summary.getErrorCount(), summary.getBatchProcessingDuration().toMillis(), summary.getRate())

        if (result.getErrors().size() > 0) {
            result.getErrors().forEach({ msg -> log.error(msg.getErrorText()) })
        }

        return result
    }

    private static void updateServerStats(MessageBatchResult result) {
        MessageBatchSummary summary = result.getBatchSummary()
        totalMessagesProcessedByServer.getAndAdd(summary.successCount)
        totalErrorsProcessedByServer.getAndAdd(summary.errorCount)
    }

    static Long getTotalMessagesProcessedByServer() {
        totalMessagesProcessedByServer.get()
    }

    static Long getTotalErrorsProcessedByServer() {
        totalErrorsProcessedByServer.get()
    }
}
