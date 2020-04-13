package com.menome.messageProcessor

import com.menome.messageBuilder.MessageBuilder
import spock.lang.Specification

class MessageProcessorSpecification extends Specification {

    static String simpleMessage = MessageBuilder.builder()
            .Name("Konrad Aust")
            .NodeType("Employee")
            .Priority(1)
            .SourceSystem("HRSystem")
            .ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
            .build()
            .toJSON()

    MessageProcessor processSimpleMessage() {
        MessageProcessor processor = new MessageProcessor()
        processor.process(simpleMessage)
        return processor
    }

    String getIndexFromList(List<String> indices, String indexName) {
        String index = ""
        indices.each() {
            if (it.contains(indexName)) {
                index = it
            }
        }
        return index
    }

    def "process invalid empty message"() {
        given:
        def msg = ""
        MessageProcessor processor = new MessageProcessor()
        processor.process(msg)
        expect:
        processor.getNeo4JStatements().isEmpty()
    }


    def "process simple valid message"() {
        given:
        def processor = processSimpleMessage()
        List<String> statements = processor.getNeo4JStatements()

        expect:
        statements.size() == 3
    }

    def "process indexes from simple message"() {
        given:
        def expectedCardIndex = "CREATE INDEX ON :Card(Email,EmployeeId)"
        def expectedEmployeeIndex = "CREATE INDEX ON :Employee(Email,EmployeeId)"
        def processor = processSimpleMessage()
        List<String> indexStatements = processor.processIndexes(simpleMessage)

        expect:
        indexStatements.size() == 2
        def actualCardIndex = getIndexFromList(indexStatements, ":Card")
        def actualEmployeeIndex = getIndexFromList(indexStatements, ":Employee")
        actualCardIndex == expectedCardIndex
        actualEmployeeIndex == expectedEmployeeIndex

    }

    def "process merge from simple message"() {
        given:
        def processor = processSimpleMessage()
        List<String> mergeStatements = processor.processMerges(simpleMessage)
        expect:
        mergeStatements.size() == 1
        mergeStatements[0] == "MERGE (node:Card:Employee {Email: \"konrad.aust@menome.com\",EmployeeId: 12345}) ON CREATE SET node.Uuid = {newUuid} SET node += {nodeParams} SET node.TheLinkAddedDate = datetime()"
    }
}
