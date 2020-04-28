package com.menome.messageProcessor

import com.menome.MessagingSpecification

class MessageProcessorSpecification extends MessagingSpecification {

    List<String> processMessage(String message) {
        MessageProcessor processor = new MessageProcessor()
        return processor.process(message)
    }

    List<String> processSimpleMessage() {
        return processMessage(simpleMessage)
    }

    List<String> processMessageWithConnections() {
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
        List<String> statements = processor.process(msg)
        expect:
        statements.isEmpty()
    }


    def "process simple valid message"() {
        given:
        List<String> statements =  processSimpleMessage()

        expect:
        statements.size() == 1
    }

    def "process message with connections"() {
        given:
        List<String> statements = processMessageWithConnections()
        expect:
        statements.size() == 7
    }

    def "process indexes from simple message"() {
        given:
        def expectedCardIndex = "CREATE INDEX ON :Card(Email,EmployeeId)"
        def expectedEmployeeIndex = "CREATE INDEX ON :Employee(Email,EmployeeId)"
        def processor = processSimpleMessage()
        List<String> indexStatements = MessageProcessor.processIndexes(simpleMessage)

        expect:
        indexStatements.size() == 2
        def actualCardIndex = getStatementFromList(indexStatements, ":Card")
        def actualEmployeeIndex = getStatementFromList(indexStatements, ":Employee")
        actualCardIndex == expectedCardIndex
        actualEmployeeIndex == expectedEmployeeIndex

    }

    def "process merge from simple message"() {
        given:
        List<String> mergeStatements = MessageProcessor.processMerges(simpleMessage)
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
        List<String> connectionStatements = MessageProcessor.processConnectionNodes(messageWithConnections)
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
        List<String> connectionStatements = MessageProcessor.processConnectionRelationships(messageWithConnections)
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
