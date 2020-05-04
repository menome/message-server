package com.menome.messageProcessor

import com.menome.MessagingSpecification

class MessageProcessorSpecification extends MessagingSpecification {

    List<String> processMessage(String message, MessageProcessor.StatementType statementType) {
        MessageProcessor processor = new MessageProcessor()
        Map<MessageProcessor.StatementType, List<String>> process = processor.process(message)
        return process.get(statementType)
    }

    List<String> processPrimaryNodeMergeForMessageWithoutConnections() {
        return processMessage(simpleMessage, MessageProcessor.StatementType.PRIMARY_NODE_MERGE)
    }

    List<String> processPrimaryNodeMergeForMessageWithConnections() {
        return processMessage(messageWithConnections, MessageProcessor.StatementType.PRIMARY_NODE_MERGE)
    }

    List<String> processIndexesForMessageWithConnections() {
        return processMessage(messageWithConnections, MessageProcessor.StatementType.INDEXES)
    }

    List<String> processConnectionMergesMessageWithConnections() {
        return processMessage(messageWithConnections, MessageProcessor.StatementType.CONNECTION_MERGE)
    }

    List<String> processConnectionMatchesMessageWithConnections() {
        return processMessage(messageWithConnections, MessageProcessor.StatementType.CONNECTION_MATCH)
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
        Map<MessageProcessor.StatementType, List<String>> statements = processor.process(msg)
        expect:
        statements.isEmpty()
    }


    def "process simple valid message"() {
        given:
        List<String> statements = processPrimaryNodeMergeForMessageWithoutConnections()

        expect:
        statements.size() == 1
    }

    def "process message with connections"() {
        given:
        List<String> statements = processPrimaryNodeMergeForMessageWithConnections()
        expect:
        statements.size() == 7
    }

    def "process indexes from simple message"() {
        given:
        def expectedCardIndex = "CREATE INDEX ON :Card(Email,EmployeeId)"
        def expectedEmployeeIndex = "CREATE INDEX ON :Employee(Email,EmployeeId)"
        List<String> indexStatements = processIndexesForMessageWithConnections()

        expect:
        indexStatements.size() == 2
        def actualCardIndex = getStatementFromList(indexStatements, ":Card")
        def actualEmployeeIndex = getStatementFromList(indexStatements, ":Employee")
        actualCardIndex == expectedCardIndex
        actualEmployeeIndex == expectedEmployeeIndex

    }

    def "process merge from simple message"() {
        given:
        List<String> mergeStatements = processPrimaryNodeMergeForMessageWithoutConnections()
        expect:
        mergeStatements.size() == 1
        mergeStatements[0] == "MERGE (employee:Card:Employee {Email: param.Email,EmployeeId: param.EmployeeId}) ON CREATE SET employee.Uuid = apoc.create.uuid(),employee.TheLinkAddedDate = datetime(), employee.SourceSystem= param.SourceSystem,employee.Priority= param.Priority,employee.Name= param.Name ON MATCH SET employee.SourceSystem= param.SourceSystem,employee.Priority= param.Priority,employee.Name= param.Name"
    }


    def "process connection merges from connection message"() {
        given:
        List<String> mergeStatements = processConnectionMergesMessageWithConnections()
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
        List<String> connectionStatements = processConnectionMatchesMessageWithConnections()
        String officeNode = getStatementFromList(connectionStatements, ":Office")
        String projectNode = getStatementFromList(connectionStatements, ":Project")
        String teamNode = getStatementFromList(connectionStatements, ":Team")
        expect:
        // We should have One Office, One Project and One Team
        connectionStatements.size() == 3
        officeNode == "MATCH (office:Office {City : param.City}) WITH employee,office"
        projectNode == "MATCH (project:Project {Code : param.Code}) WITH employee,office,project"
        teamNode == "MATCH (team:Team {Code : param.Code}) WITH employee,office,project,team"
    }

    def "process connection relationships from connection message"() {
        given:
        List<String> connectionStatements = processPrimaryNodeMergeForMessageWithConnections()
        String officeNode = getStatementFromList(connectionStatements, ":LocatedInOffice")
        String projectNode = getStatementFromList(connectionStatements, ":WorkedOnProject")
        String teamNode = getStatementFromList(connectionStatements, ":HAS_FACET")
        expect:
        officeNode == "MERGE (employee)-[office_rel:LocatedInOffice]->(office)"
        projectNode == "MERGE (employee)-[project_rel:WorkedOnProject]->(project)"
        teamNode == "MERGE (employee)-[team_rel:HAS_FACET]->(team)"
    }

    def "check all statement types"() {
        given:
        MessageProcessor processor = new MessageProcessor()
        when:
        Map<MessageProcessor.StatementType, List<String>> process = processor.process(messageWithConnections)
        then:
        process.get(MessageProcessor.StatementType.PRIMARY_NODE_MERGE).size() == 7
        process.get(MessageProcessor.StatementType.INDEXES).size() == 2
        process.get(MessageProcessor.StatementType.CONNECTION_MERGE).size() == 3
    }

    def "process parameter"() {
        given:
        String parms = MessageProcessor.processParameterJSON(messageWithConnections)
        Map<String, String> connectionParms = MessageProcessor.processParameterForConnections(messageWithConnections)
        expect:
        parms == '''{"params":{"Status":"active","Email":"konrad.aust@menome.com","Priority":1.0,"PreferredName":"The Chazzinator","EmployeeId":12345.0,"SourceSystem":"HRSystem","ResumeSkills":"programming,peeling bananas from the wrong end,handstands,sweet kickflips","Name":"Konrad Aust"}}'''
        connectionParms == [office: [Name: "Menome Victoria", NodeType: "Office", RelType: "LocatedInOffice", ForwardRel: true, City: "Victoria"], project: [Name: "theLink", NodeType: "Project", RelType: "WorkedOnProject", ForwardRel: true, Code: "5"], team: [Name: "theLink Product Team", NodeType: "Team", Label: "Facet", RelType: "HAS_FACET", ForwardRel: true, Code: "1337"]]
    }

    def "check deriveMessageTypeFromStatement"() {
        given:
        String officeMerge = "MERGE (office:Office{City: param.City}) ON CREATE SET office.Uuid = apoc.create.uuid(),office.TheLinkAddedDate = datetime(), office.Name= param.Name ON MATCH SET office.Name= param.Name"
        String nodeType = MessageProcessor.deriveMessageTypeFromStatement(officeMerge)
        expect:
        nodeType == "office"
    }

    def "check parameters for office conformed dimension - merge"() {
        given:
        String officeMerge = "MERGE (office:Office{City: param.City}) ON CREATE SET office.Uuid = apoc.create.uuid(),office.TheLinkAddedDate = datetime(), office.Name= param.Name ON MATCH SET office.Name= param.Name"
        Map<String, String> connectionParms = MessageProcessor.processParameterForConnections(messageWithConnections)
        String nodeType = MessageProcessor.deriveMessageTypeFromStatement(officeMerge)
        expect:
        Map officeParms = connectionParms[nodeType]
        officeParms.size() == 5
        officeParms.Name == "Menome Victoria"
        officeParms.NodeType == "Office"
        officeParms.RelType == "LocatedInOffice"
        officeParms.ForwardRel == Boolean.TRUE
        officeParms.City == "Victoria"
    }

    def "check parameters for office conformed dimension - match"() {
        given:
        String officeMatch = "MATCH (office:Office {City : param.City}) WITH employee,office"
        Map<String, String> connectionParms = MessageProcessor.processParameterForConnections(messageWithConnections)
        String nodeType = MessageProcessor.deriveMessageTypeFromStatement(officeMatch)
        expect:
        Map officeParms = connectionParms[nodeType]
        officeParms.size() == 5
        officeParms.Name == "Menome Victoria"
        officeParms.NodeType == "Office"
        officeParms.RelType == "LocatedInOffice"
        officeParms.ForwardRel == Boolean.TRUE
        officeParms.City == "Victoria"
    }

    def "check all parameters"() {
        given:
        Map<String, String> connectionParms = MessageProcessor.processParameterForConnections(messageWithConnections)
        expect:
        connectionParms.size() == 3
        connectionParms.office
        connectionParms.project
        connectionParms.team

        connectionParms.office.Name == "Menome Victoria"
        connectionParms.office.NodeType == "Office"
        connectionParms.office.RelType == "LocatedInOffice"
        connectionParms.office.ForwardRel == Boolean.TRUE
        connectionParms.office.City == "Victoria"

        connectionParms.project.Name == "theLink"
        connectionParms.project.NodeType == "Project"
        connectionParms.project.RelType == "WorkedOnProject"
        connectionParms.project.ForwardRel == Boolean.TRUE
        connectionParms.project.Code == "5"

        connectionParms.team.Name == "theLink Product Team"
        connectionParms.team.NodeType == "Team"
        connectionParms.team.RelType == "HAS_FACET"
        connectionParms.team.ForwardRel == Boolean.TRUE
        connectionParms.team.Code == "1337"
    }

}
