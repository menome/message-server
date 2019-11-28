
Given this message:
```json
{"Name":"Konrad Aust","NodeType":"Employee","Priority": 1,"SourceSystem": "HRSystem","ConformedDimensions": {"Email": "konrad.aust@menome.com","EmployeeId": 12345}}
```

These neo4J statements are generated
```
CREATE INDEX ON :Card(Email,EmployeeId)
CREATE INDEX ON :Employee(Email,EmployeeId)
MATCH (node:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})RETURN node;
MATCH (node:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})
RETURN node;
MERGE (node:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})ON CREATE SET node.Uuid = {newUuid}SET node += {nodeParams}SET node.TheLinkAddedDate = datetime();
MERGE (node:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})
ON CREATE SET node.Uuid = {newUuid}
SET node += {nodeParams}
SET node.TheLinkAddedDate = datetime();

{ nodeParams: 
   { Email: 'konrad.aust@menome.com',
     EmployeeId: 12345,
     Name: 'Konrad Aust',
     PendingMerge: false,
     SourceSystems: [ 'HRSystem' ],
     SourceSystemPriorities: [ 1 ],
     SourceSystemProps_HRSystem: [] },
  newUuid: 'b1914721-30eb-444c-88e7-24747c8a7d34' 
}

```