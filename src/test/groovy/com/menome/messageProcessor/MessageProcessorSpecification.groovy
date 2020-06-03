package com.menome.messageProcessor

import com.menome.MessagingSpecification
import org.everit.json.schema.ValidationException
import org.json.JSONException

class MessageProcessorSpecification extends MessagingSpecification {

    Neo4JStatements processMessage(String message) {
        MessageProcessor processor = new MessageProcessor()
        processor.process(message)
    }

    List<String> processPrimaryNodeMergeForMessageWithoutConnections() {
        return processMessage(simpleMessage).primaryNodeMerge
    }

    List<String> processPrimaryNodeMergeForMessageWithConnections() {
        return processMessage(victoriaEmployee).primaryNodeMerge
    }

    List<String> processIndexesForMessageWithConnections() {
        return processMessage(victoriaEmployee).indexes
    }

    List<String> processConnectionMergesMessageWithConnections() {
        return processMessage(victoriaEmployee).connectionMerge
    }

    List<String> processConnectionMatchesMessageWithConnections() {
        return processMessage(victoriaEmployee).connectionMatch
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
        when:
        def msg = ""
        MessageProcessor processor = new MessageProcessor()
        Neo4JStatements statements = processor.process(msg)
        then:
        !statements
    }

    def "validate empty message expect exception"(){
        when:
        def msg = ""
        MessageProcessor.validateMessage(msg)
        then:
        JSONException ex = thrown()
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
        officeNode == "MERGE (office0:Office{City: param.City}) ON CREATE SET office0.Uuid = apoc.create.uuid(),office0.TheLinkAddedDate = datetime(), office0.Name= param.Name ON MATCH SET office0.Name= param.Name"
        projectNode == "MERGE (project1:Project{Code: param.Code}) ON CREATE SET project1.Uuid = apoc.create.uuid(),project1.TheLinkAddedDate = datetime(), project1.Name= param.Name ON MATCH SET project1.Name= param.Name"
        teamNode == "MERGE (team2:Team{Code: param.Code}) ON CREATE SET team2.Uuid = apoc.create.uuid(),team2.TheLinkAddedDate = datetime(), team2.Label= param.Label,team2.Name= param.Name ON MATCH SET team2.Label= param.Label,team2.Name= param.Name"

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
        officeNode == "MATCH (office0:Office {City : param.office0City}) WITH employee,param,office0"
        projectNode == "MATCH (project1:Project {Code : param.project1Code}) WITH employee,param,office0,project1"
        teamNode == "MATCH (team2:Team {Code : param.team2Code}) WITH employee,param,office0,project1,team2"
    }

    def "process connection relationships from connection message"() {
        given:
        List<String> connectionStatements = processPrimaryNodeMergeForMessageWithConnections()
        String officeNode = getStatementFromList(connectionStatements, ":LocatedInOffice")
        String projectNode = getStatementFromList(connectionStatements, ":WorkedOnProject")
        String teamNode = getStatementFromList(connectionStatements, ":HAS_FACET")
        expect:
        officeNode == "MERGE (employee)-[office0_rel:LocatedInOffice]->(office0)"
        projectNode == "MERGE (employee)-[project1_rel:WorkedOnProject]->(project1)"
        teamNode == "MERGE (employee)-[team2_rel:HAS_FACET]->(team2)"
    }

    def "check all statement types"() {
        given:
        MessageProcessor processor = new MessageProcessor()
        when:
        Neo4JStatements statements = processor.process(victoriaEmployee)
        then:
        7 == statements.primaryNodeMerge.size()
        2 == statements.indexes.size()
        3 == statements.connectionMerge.size()
        3 == statements.connectionMatch.size()
    }

    def "process parameter"() {
        given:
        Map<String,String> parms = MessageProcessor.processPrimaryNodeParametersAsMap(victoriaEmployee)
        Map<String, String> connectionParms = MessageProcessor.processParameterForConnections(victoriaEmployee)
        expect:
        parms == [Status:"active", Email:"konrad.aust@menome.com", Priority:1.0, PreferredName:"The Chazzinator", EmployeeId:12345.0, SourceSystem:"HRSystem", ResumeSkills:"programming,peeling bananas from the wrong end,handstands,sweet kickflips", Name:"Konrad Aust"]
        connectionParms == [office0: [Name: "Menome Victoria", NodeType: "Office", RelType: "LocatedInOffice", ForwardRel: true, City: "Victoria"], project1: [Name: "theLink", NodeType: "Project", RelType: "WorkedOnProject", ForwardRel: true, Code: "5"], team2: [Name: "theLink Product Team", NodeType: "Team", Label: "Facet", RelType: "HAS_FACET", ForwardRel: true, Code: "1337"]]
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
        Map<String, String> connectionParms = MessageProcessor.processParameterForConnections(victoriaEmployee)
        String nodeType = MessageProcessor.deriveMessageTypeFromStatement(officeMerge) + "0"
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
        Map<String, String> connectionParms = MessageProcessor.processParameterForConnections(victoriaEmployee)
        String nodeType = MessageProcessor.deriveMessageTypeFromStatement(officeMatch) + "0"
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
        Map<String, String> connectionParms = MessageProcessor.processParameterForConnections(victoriaEmployee)
        expect:
        connectionParms.size() == 3
        connectionParms.office0
        connectionParms.project1
        connectionParms.team2

        connectionParms.office0.Name == "Menome Victoria"
        connectionParms.office0.NodeType == "Office"
        connectionParms.office0.RelType == "LocatedInOffice"
        connectionParms.office0.ForwardRel == Boolean.TRUE
        connectionParms.office0.City == "Victoria"

        connectionParms.project1.Name == "theLink"
        connectionParms.project1.NodeType == "Project"
        connectionParms.project1.RelType == "WorkedOnProject"
        connectionParms.project1.ForwardRel == Boolean.TRUE
        connectionParms.project1.Code == "5"

        connectionParms.team2.Name == "theLink Product Team"
        connectionParms.team2.NodeType == "Team"
        connectionParms.team2.RelType == "HAS_FACET"
        connectionParms.team2.ForwardRel == Boolean.TRUE
        connectionParms.team2.Code == "1337"
    }

    def "allow relationships with the same type within the message"(){
        given:
        def results = MessageProcessor.process(meetingMessageWithConnections)
        def primaryNodeMerge = results.primaryNodeMerge
        expect:
        1==1
    }

    def "valid message no validation exception"(){
        when:
        MessageProcessor.validateMessage(victoriaEmployee)
        then:
        noExceptionThrown()
    }


    def "invalid message should throw exception"(){
        when:
        MessageProcessor.validateMessage(invalidMessage)
        then:
        ValidationException ex = thrown()
        ex.allMessages.size() == 1
        ex.allMessages[0] == "#/NodeType: string [Invalid Node With Spaces] does not match pattern ^[a-zA-Z0-9_]*\$"
    }
}
