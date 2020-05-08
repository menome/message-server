package com.menome.datarefinery;

import com.menome.messageBatchProcessor.MessageBatchProcessor;
import com.menome.util.Neo4J;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.neo4j.driver.Driver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class DataRefineryServer {

    private static final String RABBITMQ_QUEUE_NAME = "test_queue";
    private static final int RABBITMQ_PORT = 5672;

    protected static final int NEO4J_BOLT_API_PORT = 7687;
    protected static final int NEO4J_WEB_PORT = 7474;

    static ExecutorService executor = Executors.newFixedThreadPool(10);
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


        try {
            executor.awaitTermination(20l, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Receiver rabbitReceiver = RabbitFlux.createReceiver(receiverOptions);
        consume(executor, rabbitReceiver, driver);
        log.info("Server Started");
    }

    public static void stopServer() {
        executor.shutdown();
        log.info("Server Shutdown");

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


    private static Disposable consume(ExecutorService executor, Receiver rabbitReceiver, Driver driver) {
        return rabbitReceiver.consumeAutoAck(RABBITMQ_QUEUE_NAME)
                .doOnNext(new Consumer<Delivery>() {
                    int counter = 0;

                    List<String> messages = new ArrayList<>();

                    private boolean purgeCheck(){

                        return true;
                    }

                    @Override
                    public void accept(Delivery delivery) {
                        counter++;
                        String message = new String(delivery.getBody());
                        messages.add(message);
                        log.debug(message);
                        //todo Make the batch size configurable
                        if (counter == 5000 || purgeCheck()) {
                            counter = 0;
                            List<String> messageBatch = new ArrayList<>(messages);
                            //Task task = new Task(driver, messageBatch);
                            //executor.execute(task);
                            //System.out.println("Processing Batch");
                            long start = System.nanoTime();
                            MessageBatchProcessor.process(messageBatch, driver, false);
                            long finish = System.nanoTime();

                            //System.out.println("Done Processing Batch:" + seconds);
                            log.info("elapsed = " + (finish - start) / 1000000);
                            messages = new ArrayList<>();
                        }
                        //todo: Need some way of processing smaller batches when the queue is empty maybe a countdown latch that another process can watch?
                    }
                })
                .subscribe();
    }


    /*

        println("Starting:$start")
        MessageBatchProcessor.process(fiveHundredMessageBatch, driver,true)

        def seconds =

     */

    static class Task implements Runnable {

        private final Driver driver;
        private final List<String> messages;

        public Task(Driver driver, List<String> messages) {
            this.driver = driver;
            this.messages = messages;
        }

        @Override
        public void run() {
            log.debug(Thread.currentThread().getName() + " " + messages.size());
            MessageBatchProcessor.process(messages, driver, false);
        }
    }
}