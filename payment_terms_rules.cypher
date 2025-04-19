// Update payment terms mappings with transformation rules
MATCH (s:SourceField {name: "payment terms"})
MATCH (t:TargetField {name: "zterm"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "CASE WHEN text CONTAINS 'immediate' THEN 'T000' WHEN text = '0' OR text CONTAINS 'net 0' THEN 'T001' WHEN text STARTS WITH 'net ' AND size(replace(substring(text, 4), ' ', '')) = 2 THEN 'T0' + replace(substring(text, 4), ' ', '') WHEN text STARTS WITH 'net ' AND size(replace(substring(text, 4), ' ', '')) = 3 THEN 'T' + replace(substring(text, 4), ' ', '') WHEN size(replace(text, ' ', '')) = 2 THEN 'T0' + replace(text, ' ', '') WHEN size(replace(text, ' ', '')) = 3 THEN 'T' + replace(text, ' ', '') ELSE text END";

// Update pay_terms mappings with transformation rules
MATCH (s:SourceField {name: "pay_terms"})
MATCH (t:TargetField {name: "zterm"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "CASE WHEN text CONTAINS 'immediate' THEN 'T000' WHEN text = '0' OR text CONTAINS 'net 0' THEN 'T001' WHEN text STARTS WITH 'net ' AND size(replace(substring(text, 4), ' ', '')) = 2 THEN 'T0' + replace(substring(text, 4), ' ', '') WHEN text STARTS WITH 'net ' AND size(replace(substring(text, 4), ' ', '')) = 3 THEN 'T' + replace(substring(text, 4), ' ', '') WHEN size(replace(text, ' ', '')) = 2 THEN 'T0' + replace(text, ' ', '') WHEN size(replace(text, ' ', '')) = 3 THEN 'T' + replace(text, ' ', '') ELSE text END";