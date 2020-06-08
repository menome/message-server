package com.menome.util.messageBuilder


import spock.lang.Specification

class MessageBuilderSpecification extends Specification {
    def "single property message"() {
        given:
        def message = MessageBuilder.builder().Name("Todd Costella").build()
        expect:
        message.Name == "Todd Costella"
    }

    def "all entity properties"() {
        given:
        def message = MessageBuilder.builder().Name("Todd Costella").NodeType("Employee").Priority(1).SourceSystem("HRSystem").ConformedDimensions(["Email": "ToddCostella@gmail.com", "EmployeeId": 12345]).build()
        expect:
        message.Name == "Todd Costella"
        message.NodeType == "Employee"
        message.Priority == 1
        message.SourceSystem == "HRSystem"
        message.ConformedDimensions.Email == "ToddCostella@gmail.com"
        message.ConformedDimensions.EmployeeId == 12345
    }

    def "entity as json"() {
        given:
        def message = MessageBuilder.builder().Name("Todd Costella").NodeType("Employee").Priority(1).SourceSystem("HRSystem").ConformedDimensions(["Email": "ToddCostella@gmail.com", "EmployeeId": 12345]).build()
        def json = message.toJSON()
        expect:
        json == /{"Name":"Todd Costella","NodeType":"Employee","Priority":1,"SourceSystem":"HRSystem","ConformedDimensions":{"Email":"ToddCostella@gmail.com","EmployeeId":12345}}/
    }

    def "entity with properties"() {
        given:
        def message = MessageBuilder.builder()
                .Name("Todd Costella")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("HRSystem")
                .ConformedDimensions(["Email": "ToddCostella@gmail.com", "EmployeeId": 12345])
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .build()
        def json = message.toJSON()
        expect:
        json == /{"Name":"Todd Costella","NodeType":"Employee","Priority":1,"SourceSystem":"HRSystem","ConformedDimensions":{"Email":"ToddCostella@gmail.com","EmployeeId":12345},"Properties":{"Status":"active","PreferredName":"The Chazzinator","ResumeSkills":"programming,peeling bananas from the wrong end,handstands,sweet kickflips"}}/

    }

    def "entity with properties and connections"() {
        given:

        def office = Connection.builder().Name("Menome Victoria").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).build()
        def project = Connection.builder().Name("theLink").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).build()
        def team = Connection.builder().Name("theLink Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).build()
        def message = MessageBuilder.builder()
                .Name("Todd Costella")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("HRSystem")
                .ConformedDimensions(["Email": "ToddCostella@gmail.com", "EmployeeId": 12345])
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .Connections([office, project, team])
                .build()
        def json = message.toJSON()
        expect:
        json == /{"Name":"Todd Costella","NodeType":"Employee","Priority":1,"SourceSystem":"HRSystem","ConformedDimensions":{"Email":"ToddCostella@gmail.com","EmployeeId":12345},"Properties":{"Status":"active","PreferredName":"The Chazzinator","ResumeSkills":"programming,peeling bananas from the wrong end,handstands,sweet kickflips"},"Connections":[{"Name":"Menome Victoria","NodeType":"Office","RelType":"LocatedInOffice","ForwardRel":true,"ConformedDimensions":{"City":"Victoria"}},{"Name":"theLink","NodeType":"Project","RelType":"WorkedOnProject","ForwardRel":true,"ConformedDimensions":{"Code":"5"}},{"Name":"theLink Product Team","NodeType":"Team","Label":"Facet","RelType":"HAS_FACET","ForwardRel":true,"ConformedDimensions":{"Code":"1337"}}]}/
    }

    def "pretty print comparison against menome github example"(){
        def office = Connection.builder().Name("Menome Victoria").NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).build()
        def project = Connection.builder().Name("theLink").NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).build()
        def team = Connection.builder().Name("theLink Product Team").NodeType("Team").Label("Facet").RelType("HAS_FACET").ForewardRel(true).ConformedDimensions(["Code": "1337"]).build()
        def message = MessageBuilder.builder()
                .Name("Konrad Aust")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("HRSystem")
                .ConformedDimensions(["Email": "konrad.aust@menome.com", "EmployeeId": 12345])
                .Properties(["Status": "active", "PreferredName": "The Chazzinator", "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"])
                .Connections([office, project, team])
                .build()
        def json = message.toJSONPretty()

        expect:
        json == '''{
  "Name": "Konrad Aust",
  "NodeType": "Employee",
  "Priority": 1,
  "SourceSystem": "HRSystem",
  "ConformedDimensions": {
    "Email": "konrad.aust@menome.com",
    "EmployeeId": 12345
  },
  "Properties": {
    "Status": "active",
    "PreferredName": "The Chazzinator",
    "ResumeSkills": "programming,peeling bananas from the wrong end,handstands,sweet kickflips"
  },
  "Connections": [
    {
      "Name": "Menome Victoria",
      "NodeType": "Office",
      "RelType": "LocatedInOffice",
      "ForwardRel": true,
      "ConformedDimensions": {
        "City": "Victoria"
      }
    },
    {
      "Name": "theLink",
      "NodeType": "Project",
      "RelType": "WorkedOnProject",
      "ForwardRel": true,
      "ConformedDimensions": {
        "Code": "5"
      }
    },
    {
      "Name": "theLink Product Team",
      "NodeType": "Team",
      "Label": "Facet",
      "RelType": "HAS_FACET",
      "ForwardRel": true,
      "ConformedDimensions": {
        "Code": "1337"
      }
    }
  ]
}'''
    }
}

