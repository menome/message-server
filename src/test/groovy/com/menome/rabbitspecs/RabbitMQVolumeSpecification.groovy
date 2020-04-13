package com.menome.rabbitspecs

import com.menome.test.MenomeContainerSpecification

class RabbitMQVolumeSpecification extends MenomeContainerSpecification {

    static final String RABBITMQ_TEST_EXCHANGE = "test_exchange"
    static final String RABBITMQ_TEST_ROUTING_KEY = "test_route"
    static final String RABBITMQ_QUEUE_NAME = "test_queue"

    def "write 5000 messages to rabbit"() {
        given:
        def rabbitChannel = openRabbitMQChanel(rabbitMQContainer, RABBITMQ_QUEUE_NAME, RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY)
        def msg = "Some message"
        when:
        (1..5000).each { rabbitChannel.basicPublish(RABBITMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, msg.getBytes()) }
        then:
        1 == 1
        keepContainersRunningFor10Minutes()
    }
}
