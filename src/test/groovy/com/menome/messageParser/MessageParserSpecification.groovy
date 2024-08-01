package com.menome.messageParser

import com.menome.MessagingSpecification
import com.menome.message.Neo4JConnection
import com.menome.message.Neo4JNode

class MessageParserSpecification extends MessagingSpecification {

    def "Single node from simple message"() {
        given:
        def nodes = ConvertJSonToNeo4JObjects.fromJson(simpleMessage).first()
        expect:
        nodes
        1 == nodes.size()
        Neo4JNode node = nodes[0]
        node.nodeType == "Employee"
        node.priority == 1
        node.sourceSystem == "menome_test_framework"
        node.name == "Konrad Aust"
        node.conformedDimensions
        node.conformedDimensions.Email == "konrad.aust@menome.com"
        node.conformedDimensions.EmployeeId == 12345
    }

    def "Calgary employee node with three connection nodes"() {
        given:
        def nodes = ConvertJSonToNeo4JObjects.fromJson(calgaryEmployee).first()
        expect:
        nodes
        4 == nodes.size()
        1 == nodes.findAll { node -> node.nodeType == "Employee" }.size()
        1 == nodes.findAll { node -> node.nodeType == "Project" }.size()
        1 == nodes.findAll { node -> node.nodeType == "Office" }.size()
        1 == nodes.findAll { node -> node.nodeType == "Team" }.size()
    }

    def "Calgary employee node with three connections"() {
        given:
        def connections = ConvertJSonToNeo4JObjects.fromJson(calgaryEmployee).getSecond()
        expect:
        connections
        3 == connections.size()
        1 == connections.findAll { Neo4JConnection connection -> connection.relationshipName == "LocatedInOffice" }.size()
        1 == connections.findAll { Neo4JConnection connection -> connection.relationshipName == "WorkedOnProject" }.size()
        1 == connections.findAll { Neo4JConnection connection -> connection.relationshipName == "HAS_FACET" }.size()
    }

    def "Node equality test"() {
        given:
        def node1 = ConvertJSonToNeo4JObjects.fromJson(simpleMessage).first()[0]
        def node2 = ConvertJSonToNeo4JObjects.fromJson(simpleMessage).first()[0]
        expect:
        node1 == node2
    }

    def "Node inequality test"() {
        given:
        def node1 = ConvertJSonToNeo4JObjects.fromJson(simpleMessage).first()[0]
        def node2 = ConvertJSonToNeo4JObjects.fromJson(simpleMessage).first()[0]
        node2.nodeType = "SomeOtherType"
        expect:
        node1 != node2
    }

    def "Merge statement from simple node"() {
        given:
        Neo4JNode node = ConvertJSonToNeo4JObjects.fromJson(simpleMessage).first()[0]
        def merge = node.toCypher()
        expect:
        merge
        merge == "MERGE (employee:Employee {Email: param.Email,EmployeeId: param.EmployeeId}) ON CREATE SET employee.Uuid = apoc.create.uuid(),employee.TheLinkAddedDate = datetime(), employee.Priority= param.Priority,employee.SourceSystem= param.SourceSystem,employee.Name= param.Name ON MATCH SET employee.Priority= param.Priority,employee.SourceSystem= param.SourceSystem,employee.Name= param.Name"

    }

    def "Merge statement from employee node"() {
        given:
        Neo4JNode employeeNode = ConvertJSonToNeo4JObjects.fromJson(calgaryEmployee).first().find { Neo4JNode node -> node.nodeType == "Employee" } as Neo4JNode
        def employeeMerge = employeeNode.toCypher()
        expect:
        employeeMerge
        employeeMerge == "MERGE (employee:Employee {Email: param.Email,EmployeeId: param.EmployeeId}) ON CREATE SET employee.Uuid = apoc.create.uuid(),employee.TheLinkAddedDate = datetime(), employee.Status= param.Status,employee.Priority= param.Priority,employee.PreferredName= param.PreferredName,employee.SourceSystem= param.SourceSystem,employee.ResumeSkills= param.ResumeSkills,employee.Name= param.Name ON MATCH SET employee.Status= param.Status,employee.Priority= param.Priority,employee.PreferredName= param.PreferredName,employee.SourceSystem= param.SourceSystem,employee.ResumeSkills= param.ResumeSkills,employee.Name= param.Name"
    }

    def "Merge statement from project connection"() {
        given:
        Neo4JNode projectNode = ConvertJSonToNeo4JObjects.fromJson(calgaryEmployee).first().find { Neo4JNode node -> node.nodeType == "Project" } as Neo4JNode
        def projectMerge = projectNode.toCypher()
        expect:
        projectMerge
        projectMerge == "MERGE (project:Project {Code: param.Code}) ON CREATE SET project.Uuid = apoc.create.uuid(),project.TheLinkAddedDate = datetime(), project.SourceSystem= param.SourceSystem,project.Name= param.Name ON MATCH SET project.SourceSystem= param.SourceSystem,project.Name= param.Name"
    }

    def "Merge connection statement from employee node"() {
        given:
        Neo4JConnection connection = ConvertJSonToNeo4JObjects.fromJson(calgaryEmployee).getSecond().find { Neo4JConnection connection -> connection.relationshipName == "LocatedInOffice" }
        def connectionMerge = connection.toCypher()
        expect:
        connectionMerge == "MATCH (source:Employee),(destination:Office) WHERE source.Email = param.sourceEmail AND source.EmployeeId = param.sourceEmployeeId AND destination.City = param.destinationCity MERGE (source)-[r:LocatedInOffice]->(destination) SET r = param.Properties"
    }

    def "Merge connection statement with reverse relationship from employee node"() {
        given:
        Neo4JConnection connection = ConvertJSonToNeo4JObjects.fromJson(calgaryEmployee).getSecond().find { Neo4JConnection connection -> connection.relationshipName == "LocatedInOffice" }
        connection.forwardRelationship = false
        def connectionMerge = connection.toCypher()
        expect:
        connectionMerge == "MATCH (source:Employee),(destination:Office) WHERE source.Email = param.sourceEmail AND source.EmployeeId = param.sourceEmployeeId AND destination.City = param.destinationCity MERGE (source)<-[r:LocatedInOffice]-(destination) SET r = param.Properties"
    }

    def "Merge connection with same node type avoiding parameter collision"() {
        given:
        Neo4JNode source = ConvertJSonToNeo4JObjects.fromJson(calgaryEmployee).first().find { Neo4JNode node -> node.nodeType == "Employee" }
        Neo4JNode destination = ConvertJSonToNeo4JObjects.fromJson(victoriaEmployee).first().find { Neo4JNode node -> node.nodeType == "Employee" }
        Neo4JConnection connection = new Neo4JConnection()
        connection.source = source
        connection.destination = destination
        connection.relationshipName = "Works With"
        connection.forwardRelationship = true
        String connectionMerge = connection.toCypher()
        expect:
        connectionMerge == "MATCH (source:Employee),(destination:Employee) WHERE source.Email = param.sourceEmail AND source.EmployeeId = param.sourceEmployeeId AND destination.Email = param.destinationEmail AND destination.EmployeeId = param.destinationEmployeeId MERGE (source)-[r:Works With]->(destination) SET r = param.Properties"
    }
}
