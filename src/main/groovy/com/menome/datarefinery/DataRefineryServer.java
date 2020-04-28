package com.menome.datarefinery;

import com.menome.messageProcessor.MessageProcessor;
import com.menome.util.Neo4J;
import com.rabbitmq.client.*;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.lang.Thread.sleep;

public class DataRefineryServer {

    private static final String RABBITMQ_QUEUE_NAME = "test_queue";
    protected static final String RABBITMQ_TEST_EXCHANGE = "test_exchange";
    protected static final String RABBITMQ_TEST_ROUTING_KEY = "test_route";

    private static final int RABBITMQ_PORT = 5672;

    protected static final int NEO4J_BOLT_API_PORT = 7687;
    protected static final int NEO4J_WEB_PORT = 7474;


    public static void main(String[] args) {

        System.out.println("Starting Server...");

        GenericContainer neo4JContainer = createAndStartNeo4JContainer(Network.newNetwork());

        ConnectionFactory rabbitConnectionFactory = createRabbitConnectionFactory();
        Driver driver = Neo4J.openDriver(neo4JContainer);

        try {
            Channel rabbitChannel = openRabbitMQChanel(rabbitConnectionFactory, RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY);
            rabbitChannel.basicConsume(RABBITMQ_QUEUE_NAME, new DefaultConsumer(rabbitChannel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag()
                MessageProcessor processor = new MessageProcessor()
                Session session = neo4JDriver.session()

                processor.process(new String(body))
                //println new String(body)
                List<String> statements = processor.getNeo4JStatements()
                Neo4J.executeStatementListInSession(statements, session)
                session.close()

                rabbitChannel.basicAck(deliveryTag, false)
            }
        })
        } catch (Exception e) {
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

    protected static Channel openRabbitMQChanel(ConnectionFactory rabbitConnectionFactory, String queue, String exchange, String routingKey) throws IOException, TimeoutException {
        Connection rabbitConnection = rabbitConnectionFactory.newConnection();
        Channel rabbitChannel = rabbitConnection.createChannel();
        rabbitChannel.queueDeclare(queue, true, false, false, null);
        rabbitChannel.exchangeDeclare(exchange, "topic", true);
        rabbitChannel.queueBind(queue, exchange, routingKey);

        return rabbitChannel;
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

        System.out.println("Neo4J - Bolt bolt://localhost:" + neo4JContainer.getMappedPort(NEO4J_BOLT_API_PORT));
        System.out.println("Neo4J - Web http://localhost:" + neo4JContainer.getMappedPort(NEO4J_WEB_PORT));

        return neo4JContainer;
    }


    private static Disposable consume(Receiver rabbitReceiver, Driver driver) {
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
