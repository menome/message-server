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
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;

import java.time.Duration;
import java.util.List;

import static java.lang.Thread.sleep;

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
                //.connectionSubscriptionScheduler(Schedulers.elastic());
                .connectionSubscriptionScheduler(Schedulers.parallel());

        Driver driver = Neo4J.openDriver(neo4JContainer);

        Receiver rabbitReceiver = RabbitFlux.createReceiver(receiverOptions);
        consume(rabbitReceiver,driver);

        try {
            System.out.println("Waiting....");
            sleep(10000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static ConnectionFactory createRabbitConnectionFactory() {
        ConnectionFactory rabbitConnectionFactory = new ConnectionFactory();
        rabbitConnectionFactory.setHost("127.0.0.1");
        rabbitConnectionFactory.setPort(RABBITMQ_PORT);
        rabbitConnectionFactory.setUsername("menome");
        rabbitConnectionFactory.setPassword("menome");

        return rabbitConnectionFactory;
    }

    protected static GenericContainer createAndStartNeo4JContainer(Network network) {
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

        System.out.println( "Neo4J - Bolt bolt://localhost:" + neo4JContainer.getMappedPort(NEO4J_BOLT_API_PORT));
        System.out.println( "Neo4J - Web http://localhost:" + neo4JContainer.getMappedPort(NEO4J_WEB_PORT));

        return neo4JContainer;
    }



    private static Disposable consume(Receiver rabbitReceiver,Driver driver) {
        return rabbitReceiver.consumeAutoAck(RABBITMQ_QUEUE_NAME)
                .subscribe(m -> {
                    byte[] body = m.getBody();
                    String msg = new String(body);
                    System.out.println(Thread.currentThread().getName() + " " + msg);
                    List<String> statements = MessageProcessor.process(msg);
                    Session session = driver.session();
                    Neo4J.executeStatementListInSession(statements, session);
                    session.close();
                });
    }
}
