// First clean up all existing relationships
MATCH ()-[r:MAPPED_TO]->() DELETE r;

// Fix the duplicate stcd1 target field
MATCH (t:TargetField {name: 'stcd1'}) DELETE t;
MERGE (t:TargetField {name: 'stcd1'})
SET t.type = 'char', t.length = 16;

// Create all mappings according to SourceTargetCustomerMasterRelationship.csv
MATCH (s:SourceField {name: 'customercode'})
MATCH (t:TargetField {name: 'kunnr'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'name'})
MATCH (t:TargetField {name: 'name1'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'address'})
MATCH (t:TargetField {name: 'stras'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'city'})
MATCH (t:TargetField {name: 'ort01'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'state'})
MATCH (t:TargetField {name: 'regio'})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = 'transform',
    r.transform_query = "CASE 
        WHEN toUpper(text) IN ['ALABAMA', 'AL'] THEN 'AL'
        WHEN toUpper(text) IN ['ALASKA', 'AK'] THEN 'AK'
        WHEN toUpper(text) IN ['ARIZONA', 'AZ'] THEN 'AZ'
        WHEN toUpper(text) IN ['ARKANSAS', 'AR'] THEN 'AR'
        WHEN toUpper(text) IN ['CALIFORNIA', 'CA'] THEN 'CA'
        WHEN toUpper(text) IN ['TEXAS', 'TX'] THEN 'TX'
        WHEN size(text) = 2 THEN toUpper(text)
        ELSE 'XX'
    END";

MATCH (s:SourceField {name: 'country'})
MATCH (t:TargetField {name: 'land1'})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = 'transform',
    r.transform_query = "CASE 
        WHEN toUpper(text) IN ['USA', 'US'] THEN 'USA'
        ELSE toUpper(text)
    END";

MATCH (s:SourceField {name: 'payment terms'})
MATCH (t:TargetField {name: 'zterm'})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = 'transform',
    r.transform_query = "CASE 
        WHEN text CONTAINS 'immediate' THEN 'T000'
        WHEN text = '0' OR text CONTAINS 'net 0' THEN 'T001'
        WHEN text STARTS WITH 'net ' AND size(replace(substring(text, 4), ' ', '')) = 2 
        THEN 'T0' + replace(substring(text, 4), ' ', '')
        WHEN text STARTS WITH 'net ' AND size(replace(substring(text, 4), ' ', '')) = 3
        THEN 'T' + replace(substring(text, 4), ' ', '')
        WHEN size(replace(text, ' ', '')) = 2 
        THEN 'T0' + replace(text, ' ', '')
        WHEN size(replace(text, ' ', '')) = 3
        THEN 'T' + replace(text, ' ', '')
        ELSE text
    END";

MATCH (s:SourceField {name: 'taxid'})
MATCH (t:TargetField {name: 'stcd1'})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = 'transform',
    r.transform_query = "CASE 
        WHEN NOT text CONTAINS '-' AND size(text) >= 7 
        THEN substring(text, 0, 2) + '-' + substring(text, 2)
        ELSE text
    END";

MATCH (s:SourceField {name: 'credit_limit'})
MATCH (t:TargetField {name: 'klimk'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'credit rating'})
MATCH (t:TargetField {name: 'kdgrp'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'type'})
MATCH (t:TargetField {name: 'ktokd'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'sale org'})
MATCH (t:TargetField {name: 'vkorg'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'channel'})
MATCH (t:TargetField {name: 'vtweg'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);

MATCH (s:SourceField {name: 'division'})
MATCH (t:TargetField {name: 'spart'})
MERGE (s)-[:MAPPED_TO {rule: 'direct'}]->(t);