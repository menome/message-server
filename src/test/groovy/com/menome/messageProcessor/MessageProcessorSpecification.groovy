package com.menome.messageProcessor

import com.menome.MessagingSpecification

class MessageProcessorSpecification extends MessagingSpecification {

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
        statements.size() == 1
    }

    def "process message with connections"() {
        given:
        def processor = processMessageWithConnections()
        List<String> statements = processor.getNeo4JStatements()
        expect:
        statements.size() == 7
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
        mergeStatements[0] == "MERGE (employee:Card:Employee {Email: \"konrad.aust@menome.com\",EmployeeId: 12345}) ON CREATE SET employee.Uuid = apoc.create.uuid(),employee.TheLinkAddedDate = datetime(), employee.Name= \"Konrad Aust\",employee.Priority= 1,employee.SourceSystem= \"HRSystem\" ON MATCH SET employee.Name= \"Konrad Aust\",employee.Priority= 1,employee.SourceSystem= \"HRSystem\""
    }

    def "process node parameters from simple message"() {
        expect:
        true
    }

    def "process connection nodes from connection message"() {
        given:
        def processor = processMessageWithConnections()
        List<String> connectionStatements = processor.processConnectionNodes(messageWithConnections)
        String officeNode = getStatementFromList(connectionStatements, ":Office")
        String projectNode = getStatementFromList(connectionStatements, ":Project")
        String teamNode = getStatementFromList(connectionStatements, ":Team")
        expect:
        // We should have One Office, One Project and One Team
        connectionStatements.size() == 3
        officeNode == "MERGE (office:Card:Office {City: \"Victoria\"}) ON CREATE SET office.Uuid = apoc.create.uuid(),office.TheLinkAddedDate = datetime(), office.Name = \"Menome Victoria\" , office.PendingMerge = true"
        projectNode == "MERGE (project:Card:Project {Code: 5}) ON CREATE SET project.Uuid = apoc.create.uuid(),project.TheLinkAddedDate = datetime(), project.Name = \"theLink\" , project.PendingMerge = true"
        teamNode == "MERGE (team:Card:Team {Code: 1337}) ON CREATE SET team.Uuid = apoc.create.uuid(),team.TheLinkAddedDate = datetime(), team.Name = \"theLink Product Team\" , team.PendingMerge = true"
    }

    def "process connection relationships from connection message"() {
        given:
        def processor = processMessageWithConnections()
        List<String> connectionStatements = processor.processConnectionRelationships(messageWithConnections)
        String officeNode = getStatementFromList(connectionStatements, ":LocatedInOffice")
        String projectNode = getStatementFromList(connectionStatements, ":WorkedOnProject")
        String teamNode = getStatementFromList(connectionStatements, ":HAS_FACET")
        expect:
        // We should have One Office, One Project and One Team
        connectionStatements.size() == 3
        officeNode == "MERGE (employee)-[office_rel:LocatedInOffice]->(office)"
        projectNode == "MERGE (employee)-[project_rel:WorkedOnProject]->(project)"
        teamNode == "MERGE (employee)-[team_rel:HAS_FACET]->(team)"
    }
}
