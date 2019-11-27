package com.menome.messageBuilder


import groovy.json.JsonOutput
import spock.lang.Specification

class MessageBuilderSpecification extends Specification {
    def "single property message"() {
        given:
        def message = MessageBuilder.builder().Name("Todd Costella").build()
        expect:
        message.Name == "Todd Costella"
    }

    def "all entity properties"(){
        given:
        def message = MessageBuilder.builder().Name("Todd Costella").NodeType("Employee").Priority(1).SourceSystem("HRSystem").ConformedDimensions(["Email":"ToddCostella@gmail.com","EmployeeId":12345]).build()
        expect:
        message.Name == "Todd Costella"
        message.NodeType == "Employee"
        message.Priority == 1
        message.SourceSystem == "HRSystem"
        message.ConformedDimensions.Email == "ToddCostella@gmail.com"
        message.ConformedDimensions.EmployeeId == 12345
    }

    def "entity as json"(){
        given:
        def message = MessageBuilder.builder().Name("Todd Costella").NodeType("Employee").Priority(1).SourceSystem("HRSystem").ConformedDimensions(["Email":"ToddCostella@gmail.com","EmployeeId":12345]).build()
        def json = JsonOutput.toJson(message)
        expect:
        json == /{"sourceSystem":"HRSystem","nodeType":"Employee","conformedDimensions":{"Email":"ToddCostella@gmail.com","EmployeeId":12345},"priority":1,"name":"Todd Costella"}/



    }
}

