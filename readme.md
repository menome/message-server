
Given this message:
```json
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

These neo4J statements are generated
```
MERGE (node:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})
ON CREATE SET node.Uuid = {newUuid}
SET node += {nodeParams}
SET node.TheLinkAddedDate = datetime()

MERGE (node0:Card:Office {City: "Victoria"})
ON CREATE SET node0.Uuid = {node0_newUuid}, node0.PendingMerge = true

MERGE (node)-[node0_rel:LocatedInOffice]->(node0)
SET node0_rel += {node0_relProps}, node0 += {node0_nodeParams}

MERGE (node1:Card:Project {Code: "5"})
ON CREATE SET node1.Uuid = {node1_newUuid}, node1.PendingMerge = true

MERGE (node)-[node1_rel:WorkedOnProject]->(node1)
SET node1_rel += {node1_relProps}, node1 += {node1_nodeParams}

MERGE (node2:Facet:Team {Code: "1337"})
ON CREATE SET node2.Uuid = {node2_newUuid}, node2.PendingMerge = true

MERGE (node)-[node2_rel:HAS_FACET]->(node2)
SET node2_rel += {node2_relProps}, node2 += {node2_nodeParams};
```
Parameters
```
{ node0_newUuid: '142d3af5-21ee-4f86-934e-6ed2a7368b23',
  node0_nodeParams: { City: 'Victoria', Name: 'Menome Victoria' },
  node0_relProps: {},
  node1_newUuid: 'c729e88b-7c2a-45ed-90ed-7dfd4a91de21',
  node1_nodeParams: { Code: '5', Name: 'theLink' },
  node1_relProps: {},
  node2_newUuid: '0b7cd56d-c323-4548-8c68-eb69d96665d8',
  node2_nodeParams: { Code: '1337', Name: 'theLink Product Team' },
  node2_relProps: {},
  nodeParams: 
   { Status: 'active',
     PreferredName: 'The Chazzinator',
     ResumeSkills: 'programming,peeling bananas from the wrong end,handstands,sweet kickflips',
     Email: 'konrad.aust@menome.com',
     EmployeeId: 12345,
     Name: 'Konrad Aust',
     PendingMerge: false,
     SourceSystems: [ 'HRSystem' ],
     SourceSystemPriorities: [ 1 ],
     SourceSystemProps_HRSystem: [ 'Status', 'PreferredName', 'ResumeSkills' ] },
  newUuid: 'efed44e2-9fbf-454e-a06a-be5683b2d3a8' }
```