package com.menome.neo4JIntegration

import com.menome.MessagingSpecification
import com.menome.messageProcessor.MessageProcessor
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.neo4j.driver.Result
import org.neo4j.driver.Session
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import spock.lang.Shared

class MessageToGraphSpecification extends MessagingSpecification {

    @Shared
    GenericContainer neo4JContainer

    @Shared
    Driver neo4JDriver

    static Logger log = LoggerFactory.getLogger(MessageToGraphSpecification.class)

    Result run(String statement){
        Neo4J.run(neo4JDriver, statement,[:])
    }

    def assertOneResultWithATrueValue(String matchExpression) {
        Result result = run(matchExpression)
        def resultMap = result.single().asMap()
        resultMap.size() == 1
        resultMap.values()[0] == true
    }


    def setup(){
        run("MATCH (n) DETATCH DELETE n")
    }

    def setupSpec(){
        neo4JContainer = createAndStartNeo4JContainer(Network.newNetwork())
        neo4JDriver =  Neo4J.openDriver(neo4JContainer)
    }

    def cleanupSpec(){
        neo4JDriver.close()
    }

    def "create graph from message with relationships"() {
        given:
        MessageProcessor processor = new MessageProcessor()
        Session session = neo4JDriver.session()
        when:

        processor.process(employeeMessageWithConnections)
        List<String> statements = processor.getNeo4JStatements()
        Neo4J.executeStatementListInSession(statements, session)
        session.close()

        then:
        assertOneResultWithATrueValue("MATCH (n) RETURN COUNT(n) =                 4")
        assertOneResultWithATrueValue("Match(employee:Employee:Card) return employee.Name = \"Konrad Aust\"")
        Map employeeInOfficeResult = run("Match(employee:Employee)-[:LocatedInOffice]-(office:Office) return employee.Name,office.Name").single().asMap()
        employeeInOfficeResult."employee.Name"  == "Konrad Aust"
        employeeInOfficeResult."office.Name"  == "Menome Victoria"

        Map employeeOnProjectResult = run("Match(employee:Employee)-[:WorkedOnProject]-(project:Project) return employee.Name ,project.Name").single().asMap()
        employeeOnProjectResult."employee.Name"  == "Konrad Aust"
        employeeOnProjectResult."project.Name"  == "theLink"

        Map attributesOnEmployeeNode = run("Match (employee:Employee) return employee.Email,employee.EmployeeId,employee.Name,employee.PreferredName,employee.Priority,employee.ResumeSkills,employee.SourceSystem,employee.Status,employee.TheLinkAddedDate,employee.Uuid").single().asMap()
        attributesOnEmployeeNode."employee.Email"  == "konrad.aust@menome.com"
        attributesOnEmployeeNode."employee.EmployeeId"  == 12345
        attributesOnEmployeeNode."employee.Name"  == "Konrad Aust"
        attributesOnEmployeeNode."employee.PreferredName" == "The Chazzinator"
        attributesOnEmployeeNode."employee.Priority" == 1
        attributesOnEmployeeNode."employee.ResumeSkills" == "programming,peeling bananas from the wrong end,handstands,sweet kickflips"
        attributesOnEmployeeNode."employee.SourceSystem" == "HRSystem"
        attributesOnEmployeeNode."employee.Status" == "active"
        attributesOnEmployeeNode."employee.TheLinkAddedDate" != null
        attributesOnEmployeeNode."employee.Uuid" != null
    }


}
