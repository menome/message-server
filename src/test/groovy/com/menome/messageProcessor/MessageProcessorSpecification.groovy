package com.menome.messageProcessor

import com.menome.messageBuilder.Connection
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

    static office = Connection.builder().Name("Menome Victoria").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).build()
    static project = Connection.builder().Name("theLink").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).build()
    static team = Connection.builder().Name("theLink Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).build()

    static String messageWithConnections = MessageBuilder.builder()
            .Name("Konrad Aust")
            .NodeType("Employee")
            .Priority(1)
            .SourceSystem("HRSystem")
            .ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
            .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
            .Connections([office, project, team])
            .build()
            .toJSON()


    MessageProcessor processMessage(String message) {
        MessageProcessor processor = new MessageProcessor()
        processor.process(message)
        return processor
    }

    MessageProcessor processSimpleMessage() {
        return processMessage(simpleMessage)
    }

    MessageProcessor processMessageWithConnections() {
        return processMessage(messageWithConnections)
    }

    String getStatementFromList(List<String> list, String statementFragment) {
        String statement = ""
        list.each() {
            if (it.contains(statementFragment)) {
                statement = it
            }
        }
        return statement
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
        def actualCardIndex = getStatementFromList(indexStatements, ":Card")
        def actualEmployeeIndex = getStatementFromList(indexStatements, ":Employee")
        actualCardIndex == expectedCardIndex
        actualEmployeeIndex == expectedEmployeeIndex

    }

    def "process merge from simple message"() {
        given:
        def processor = processSimpleMessage()
        List<String> mergeStatements = processor.processMerges(simpleMessage)
        expect:
        mergeStatements.size() == 1
        mergeStatements[0] == "MERGE (node:Card:Employee {Email: \"konrad.aust@menome.com\",EmployeeId: 12345}) ON CREATE SET node.Uuid = apoc.create.uuid(),node.TheLinkAddedDate = datetime() SET node += {nodeParams}"
    }

    def "process node parameters from simple message"() {
        expect:
        true
    }

    def "process connection nodes from connection message"() {
        given:
        def processor = processMessageWithConnections()
        List<String> connectionStatements = processor.processConnectedNodes(messageWithConnections)
        String officeNode = getStatementFromList(connectionStatements,":Office")
        String projectNode = getStatementFromList(connectionStatements,":Project")
        String teamNode = getStatementFromList(connectionStatements,":Team")
        expect:
        // We should have One Office, One Project and One Team
        connectionStatements.size() == 3
        //todo: The node suffix (0,1,2) could be associated with different nodes depending on the order they are created. They seem to be consistent but there is no guarantee
        officeNode ==  "MERGE (node0:Card:Office {City: \"Victoria\"}) ON CREATE SET node0.Uuid = apoc.create.uuid(),node0.TheLinkAddedDate = datetime() SET node0.PendingMerge = true"
        projectNode == "MERGE (node1:Card:Project {Code: 5}) ON CREATE SET node1.Uuid = apoc.create.uuid(),node1.TheLinkAddedDate = datetime() SET node1.PendingMerge = true"
        teamNode == "MERGE (node2:Card:Team {Code: 1337}) ON CREATE SET node2.Uuid = apoc.create.uuid(),node2.TheLinkAddedDate = datetime() SET node2.PendingMerge = true"
        true
    }
}
