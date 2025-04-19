// Add all source fields with exact CSV names
MERGE (s1:SourceField {name: "name"})
SET s1.type = "char", s1.length = 50;

MERGE (s2:SourceField {name: "address"})
SET s2.type = "char", s2.length = 45;

MERGE (s3:SourceField {name: "customercode"})
SET s3.type = "char", s3.length = 10;

MERGE (s4:SourceField {name: "country"})
SET s4.type = "char", s4.length = 3;

MERGE (s5:SourceField {name: "city"})
SET s5.type = "char", s5.length = 35;

MERGE (s6:SourceField {name: "state"})
SET s6.type = "char", s6.length = 3;

MERGE (s7:SourceField {name: "payment terms"})
SET s7.type = "char", s7.length = 4;

MERGE (s8:SourceField {name: "type"})
SET s8.type = "char", s8.length = 4;

MERGE (s9:SourceField {name: "sale org"})
SET s9.type = "char", s9.length = 4;

MERGE (s10:SourceField {name: "channel"})
SET s10.type = "char", s10.length = 2;

MERGE (s11:SourceField {name: "division"})
SET s11.type = "char", s11.length = 4;

MERGE (s12:SourceField {name: "taxid"})
SET s12.type = "char", s12.length = 16;

MERGE (s13:SourceField {name: "credit_limit"})
SET s13.type = "char", s13.length = 18;

MERGE (s14:SourceField {name: "credit rating"})
SET s14.type = "char", s14.length = 2;

MERGE (s15:SourceField {name: "pay_terms"})
SET s15.type = "char", s15.length = 4;

// Add all target fields
MERGE (t1:TargetField {name: "bankn"})
SET t1.type = "char", t1.length = 18;

MERGE (t2:TargetField {name: "bkont"})
SET t2.type = "char", t2.length = 2;

MERGE (t3:TargetField {name: "kunnr"})
SET t3.type = "char", t3.length = 10;

MERGE (t4:TargetField {name: "name1"})
SET t4.type = "char", t4.length = 35;

MERGE (t5:TargetField {name: "ort01"})
SET t5.type = "char", t5.length = 35;

MERGE (t6:TargetField {name: "stras"})
SET t6.type = "char", t6.length = 35;

MERGE (t7:TargetField {name: "regio"})
SET t7.type = "char", t7.length = 3;

MERGE (t8:TargetField {name: "banks"})
SET t8.type = "char", t8.length = 3;

MERGE (t9:TargetField {name: "ktokd"})
SET t9.type = "char", t9.length = 4;

MERGE (t10:TargetField {name: "stcd1"})
SET t10.type = "char", t10.length = 16;

MERGE (t11:TargetField {name: "txjcd"})
SET t11.type = "char", t11.length = 4;

MERGE (t12:TargetField {name: "sperr"})
SET t12.type = "char", t12.length = 2;

MERGE (t13:TargetField {name: "zterm"})
SET t13.type = "char", t13.length = 4;

// Create mappings with exact CSV field names
MATCH (s:SourceField {name: "name"})
MATCH (t:TargetField {name: "name1"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "customercode"})
MATCH (t:TargetField {name: "kunnr"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "address"})
MATCH (t:TargetField {name: "ort01"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "city"})
MATCH (t:TargetField {name: "stras"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "state"})
MATCH (t:TargetField {name: "regio"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "country"})
MATCH (t:TargetField {name: "banks"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "type"})
MATCH (t:TargetField {name: "ktokd"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "taxid"})
MATCH (t:TargetField {name: "stcd1"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "division"})
MATCH (t:TargetField {name: "txjcd"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "channel"})
MATCH (t:TargetField {name: "sperr"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "payment terms"})
MATCH (t:TargetField {name: "zterm"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "
    CASE
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

MATCH (s:SourceField {name: "credit_limit"})
MATCH (t:TargetField {name: "bankn"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "credit rating"})
MATCH (t:TargetField {name: "bkont"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "pay_terms"})
MATCH (t:TargetField {name: "zterm"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "
    CASE
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

