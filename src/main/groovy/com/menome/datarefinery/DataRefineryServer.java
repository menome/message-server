package com.menome.datarefinery;

import com.menome.messageProcessor.MessageProcessor;
import com.menome.util.Neo4J;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import reactor.core.Disposable;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataRefineryServer {

    private static final String RABBITMQ_QUEUE_NAME = "test_queue";
    private static final int RABBITMQ_PORT = 5672;

    protected static final int NEO4J_BOLT_API_PORT = 7687;
    protected static final int NEO4J_WEB_PORT = 7474;


    public static void main(String[] args) {

        System.out.println("Starting Server...");

        GenericContainer neo4JContainer = createAndStartNeo4JContainer(Network.newNetwork());

        ConnectionFactory rabbitConnectionFactory = createRabbitConnectionFactory();

        ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(rabbitConnectionFactory)
                .connectionSupplier(cf -> cf.newConnection(new Address[]{new Address("127.0.0.1")}, RABBITMQ_QUEUE_NAME))
                ;

        Driver driver = Neo4J.openDriver(neo4JContainer);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            executor.awaitTermination(20l, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Receiver rabbitReceiver = RabbitFlux.createReceiver(receiverOptions);
        consume(executor, rabbitReceiver, driver);

        /*
        Todo: Need some way to shutdown the executor service. Message on a queue?
        executor.shutdown();
        executor.shutdownNow();
         */
    }


    private static ConnectionFactory createRabbitConnectionFactory() {
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
        rabbitConnectionFactory.setHost("127.0.0.1");
        rabbitConnectionFactory.setPort(RABBITMQ_PORT);
        rabbitConnectionFactory.setUsername("menome");
        rabbitConnectionFactory.setPassword("menome");

        return rabbitConnectionFactory;
    }

    private static GenericContainer createAndStartNeo4JContainer(Network network) {
        GenericContainer neo4JContainer = new GenericContainer("neo4j:4.0.3")
                .withNetwork(network)
                .withNetworkAliases("neo4j")
                .withExposedPorts(NEO4J_WEB_PORT)
                .withExposedPorts(NEO4J_BOLT_API_PORT)
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withEnv("NEO4J_AUTH", "neo4j/password")
                .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")
                .withStartupTimeout(Duration.ofMinutes(5));

        neo4JContainer.start();

        System.out.println("Neo4J - Bolt bolt://localhost:" + neo4JContainer.getMappedPort(NEO4J_BOLT_API_PORT));
        System.out.println("Neo4J - Web http://localhost:" + neo4JContainer.getMappedPort(NEO4J_WEB_PORT));

        return neo4JContainer;
    }


    private static Disposable consume(ExecutorService executor, Receiver rabbitReceiver, Driver driver) {
        return rabbitReceiver.consumeAutoAck(RABBITMQ_QUEUE_NAME)
                .subscribe(m -> {
                    byte[] body = m.getBody();
                    String msg = new String(body);
                    Task task = new Task(driver,msg);
                    executor.submit(task);
                });
    }

    static class Task implements Runnable {

        private final Driver driver;
        private final String message;

        public Task(Driver driver,String message) {
            this.driver = driver;
            this.message = message;
        }

        @Override
        public void run() {
            //System.out.println(Thread.currentThread().getName() + " " + message);
            if (message.contains("t1@") || message.contains("t5000@")){
                System.out.println("Time:" + Instant.now());
            }
            List<String> statements = MessageProcessor.process(message);
            Session session = driver.session();
            Neo4J.executeStatementListInSession(statements, session);
            session.close();
        }
    }
}