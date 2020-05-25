#Overview
This project reads messages in a specific structure from a RabbitMQ queue and converts them into Neo4J database graph updates 

#Example
```json5
  {
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
  }
```

```
CREATE INDEX ON :Employee(Email,EmployeeId)
CREATE INDEX ON :Card(Email,EmployeeId)

MERGE (office0:Office{City: param.City}) ON CREATE SET office0.Uuid = apoc.create.uuid(),office0.TheLinkAddedDate = datetime(), office0.Name= param.Name ON MATCH SET office0.Name= param.Name
MERGE (project1:Project{Code: param.Code}) ON CREATE SET project1.Uuid = apoc.create.uuid(),project1.TheLinkAddedDate = datetime(), project1.Name= param.Name ON MATCH SET project1.Name= param.Name
MERGE (team2:Team{Code: param.Code}) ON CREATE SET team2.Uuid = apoc.create.uuid(),team2.TheLinkAddedDate = datetime(), team2.Label= param.Label,team2.Name= param.Name ON MATCH SET team2.Label= param.Label,team2.Name= param.Name

MERGE (employee:Card:Employee {Email: param.Email,EmployeeId: param.EmployeeId}) ON CREATE SET employee.Uuid = apoc.create.uuid(),employee.TheLinkAddedDate = datetime(), employee.Status= param.Status,employee.Priority= param.Priority,employee.PreferredName= param.PreferredName,employee.SourceSystem= param.SourceSystem,employee.ResumeSkills= param.ResumeSkills,employee.Name= param.Name ON MATCH SET employee.Status= param.Status,employee.Priority= param.Priority,employee.PreferredName= param.PreferredName,employee.SourceSystem= param.SourceSystem,employee.ResumeSkills= param.ResumeSkills,employee.Name= param.Name WITH employee,param 
MATCH (office0:Office {City : param.office0City}) WITH employee,param,office0
MATCH (project1:Project {Code : param.project1Code}) WITH employee,param,office0,project1
MATCH (team2:Team {Code : param.team2Code}) WITH employee,param,office0,project1,team2
MERGE (employee)-[office0_rel:LocatedInOffice]->(office0)
MERGE (employee)-[project1_rel:WorkedOnProject]->(project1)
MERGE (employee)-[team2_rel:HAS_FACET]->(team2)
```


![Three Node Employee Graph](doc/assets/ThreeNodeEmployeeGraph.png)

## Objectives
The primary objective of this project is to update the graph as fast as possible. 
- Messages are streamed off of the Rabbit queue into batches. The batch size defaults to 5,000 but can be configured (see Environment/Configuration Variables below.) 
- Each batch is processed in its own thread. 

## Principals

####Immutability 
Given that each batch will be processed by a thread, I wanted to ensure there was no state kept in the objects. To that end, I opted to adopt some principals from the functional programming world. The first one being immutability. In my processing framework, once an object is created it's not modified. This is done by convention as there aren't too many immutable data structures in Java/Groovy. There are other pure functional languages implemented on the JVM Clojure (Lisp based) and Frege(a Haskel) being two of the more popular functional languages. Scala is another option but it's more of an Object/Functional hybrid. 

####Pure Functions
Another aspect of a functional approach is ensuring functions don't have side effects. That is given a set of arguments, the function is guaranteed to return the same value and won't impact the 'outside' world. There are no language semantics to express this concept in Java/Groovy so again this is by convention. I opted to implement all behaviour as static functions. This helps enforce statelessness in the objects as static functions can't access instance variables in a class. In the classes I've implemented, there aren't any instance variables so this point is moot, however it's good practice to enforce this at the class level. The system does perform logging at various levels which does violate the no side effect principal, but that's an acceptable exception in my opinion.   

####No Shared Mutable State
Given the two principals of immutability and pure functions the goal is not to have any shared state which could be mutated. This is particularly important when we have multiple threads in play. Sharing state that could be mutated in multiple threads is a recipe for disaster. 

####Test Driven
There are a number of tests that validate the correctness of the implementation. They were developed in concert with the code. In many cases, tests were written before the implementation. Tests are implemented using the Spock BDD framework (http://spockframework.org/) 
        

## Solution Overview
The message processing solution consists of three primary classes, a few utility classes and tests in the form of Spock specifications. This section describes each of the major components, how they relate and anything significant about them. 

At a high level, the system reads messages from a Rabbit MQ queue, collects the message stream into batches, converts each entry in the batch to a series of Neo43J Cypher statements and applies the batch in a single transaction to update the graph database.


![Component Diagram](doc/assets/Component Diagram.png)

### Message Server
The MessageServer (com.menome.MessageServerCommand) is the main entry point for the system. It can be launched from the command line, through an iIDE (IntelliJ for example) or packaged into a docker container. 

TODO: Command line example with system properties and/or properties files.
 

### Message Batch Processor
### Message Processor
### Utility Classes


##Technology
JVM (Java Version 11.x)
Groovy (Version:)
Spock
Micronaut
Reactor
Picocli
RabbitMQ
Neo4J
Docker
Kubernetes
Test Containers





## Environment/Configuration Variables



## Resources

####Twelve Factor Application

The principals described in https://12factor.net/ are generally what we're trying to adhere to with the Menome implementation. 

#####Codebase - One codebase tracked in revision control, many deploys
No.Each service/bot/agent has it's own project on GitHub. This project is no exception to that 

#####Dependencies - Explicitly declare and isolate dependencies
Yes. All dependencies are managed through gradle.
 
#####Config - Store config in the environment
Yes. Managed through docker and passed into the system through environment variables.

#####Backing services - Treat backing services as attached resources
Yes. Neo4J and Rabbit are implmented as such

#####Build, release, run Strictly separate build and run stages
Yes. Accomplished through Gradle

#####Processes - Execute the app as one or more stateless processes
Yes. As mentioned above, the approach is to favor immutability at the object level and all state is managed by Neo4J and Rabbit MQ

#####Port binding - Export services via port binding
Yes. Ports are configured via docker and the server is connects to the outside environment via configuration.

#####Concurrency - Scale out via the process model
Yes. The code is designed to not only run internally with a high level of concurrency, but the multiple instances of the server can be run to scale out horizontally to service higher workloads. The only constraint is that a Rabbit MQ queue can only be serviced by one server.  

#####Disposability - Maximize robustness with fast startup and graceful shutdown
Yes. The Micrononaut (https://micronaut.io) framework coupled with the picocli implementation was used to provide fast startup, but also provides other services like health and metrics. 
  
#####Dev/prod parity Keep development, staging, and production as similar as possible
Yes. The server can be run locally with docker containers or configured against standalone Neo4j and Rabbit MQ services. This is exactly how the system will be run in production. This concept is carried forward with the use of testcontainers (https://testcontaners.org)  

#####Logs - Treat logs as event streams
Yes.

XII. Admin processes
N/A
