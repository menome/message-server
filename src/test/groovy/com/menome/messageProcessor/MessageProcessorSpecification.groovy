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
        mergeStatements[0] == "MERGE (employee:Card:Employee {Email: param.Email,EmployeeId: param.EmployeeId}) ON CREATE SET employee.Uuid = apoc.create.uuid(),employee.TheLinkAddedDate = datetime(), employee.SourceSystem= param.SourceSystem,employee.Priority= param.Priority,employee.Name= param.Name ON MATCH SET employee.SourceSystem= param.SourceSystem,employee.Priority= param.Priority,employee.Name= param.Name"
    }


    def "process connection merges from connection message"() {
        given:
        List<String> mergeStatements = MessageProcessor.processConnectionMerges(messageWithConnections)
        String officeNode = getStatementFromList(mergeStatements, ":Office")
        String projectNode = getStatementFromList(mergeStatements, ":Project")
        String teamNode = getStatementFromList(mergeStatements, ":Team")

        expect:
        mergeStatements.size() == 3
        officeNode == "MERGE (office:Office{City: param.City}) ON CREATE SET office.Uuid = apoc.create.uuid(),office.TheLinkAddedDate = datetime(), office.Name= param.Name ON MATCH SET office.Name= param.Name"
        projectNode == "MERGE (project:Project{Code: param.Code}) ON CREATE SET project.Uuid = apoc.create.uuid(),project.TheLinkAddedDate = datetime(), project.Name= param.Name ON MATCH SET project.Name= param.Name"
        teamNode == "MERGE (team:Team{Code: param.Code}) ON CREATE SET team.Uuid = apoc.create.uuid(),team.TheLinkAddedDate = datetime(), team.Label= param.Label,team.Name= param.Name ON MATCH SET team.Label= param.Label,team.Name= param.Name"

    }

    def "process connection matches from connection message"() {
        given:
        List<String> connectionStatements = MessageProcessor.processConnectionNodes(messageWithConnections)
        String officeNode = getStatementFromList(connectionStatements, ":Office")
        String projectNode = getStatementFromList(connectionStatements, ":Project")
        String teamNode = getStatementFromList(connectionStatements, ":Team")
        expect:
        // We should have One Office, One Project and One Team
        connectionStatements.size() == 3
        officeNode == "MATCH (office:Office {City : param.Office.City}) WITH employee,office"
        projectNode == "MATCH (project:Project {Code : param.Project.Code}) WITH employee,office,project"
        teamNode == "MATCH (team:Team {Code : param.Team.Code}) WITH employee,office,project,team"
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

    def "process parameter"(){
        given:
        String parms = MessageProcessor.processParameterJSON(messageWithConnections)
        expect:
        parms
    }
}
