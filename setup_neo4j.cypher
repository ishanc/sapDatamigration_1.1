// Create Source Field Nodes
LOAD CSV WITH HEADERS FROM 'file:///SourceCustomerMaster.csv' AS row
MERGE (s:SourceField {name: row.SourceField})
SET s.description = row.SourceFieldDescription,
    s.type = row.SourceType,
    s.length = toInteger(row.SourceLength),
    s.predicate = row.RDFPredicate;

// Create Target Field Nodes
LOAD CSV WITH HEADERS FROM 'file:///TargetCustomerMaster.csv' AS row
MERGE (t:TargetField {name: row.TargetField})
SET t.description = row.TargetFieldDescription,
    t.type = row.TargetType,
    t.length = toInteger(row.TargetLength),
    t.key = row.TargetKey,
    t.predicate = row.RDFPredicate;

// Create Relationships Between Source and Target Fields
LOAD CSV WITH HEADERS FROM 'file:///SourceTargetCustomerMasterRelationship.csv' AS row
MATCH (s:SourceField {name: row.SourceField})
MATCH (t:TargetField {name: row.TargetField})
MERGE (s)-[r:MAPPED_TO {rule: row.MappingRule}]->(t);