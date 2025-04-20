// First, remove all existing MAPPED_TO relationships
MATCH ()-[r:MAPPED_TO]->() DELETE r;

// Create mappings according to SourceTargetCustomerMasterRelationship.csv
// For fields that need transformation, we'll set the transform rule after creating the relationship

// Direct mappings
MATCH (s:SourceField {name: "customercode"})
MATCH (t:TargetField {name: "kunnr"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "name"})
MATCH (t:TargetField {name: "name1"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "address"})
MATCH (t:TargetField {name: "stras"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "city"})
MATCH (t:TargetField {name: "ort01"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

// State with transformation
MATCH (s:SourceField {name: "state"})
MATCH (t:TargetField {name: "regio"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "CASE 
        WHEN toUpper(text) IN ['ALABAMA', 'AL'] THEN 'AL'
        WHEN toUpper(text) IN ['ALASKA', 'AK'] THEN 'AK'
        WHEN toUpper(text) IN ['ARIZONA', 'AZ'] THEN 'AZ'
        WHEN toUpper(text) IN ['ARKANSAS', 'AR'] THEN 'AR'
        WHEN toUpper(text) IN ['CALIFORNIA', 'CA'] THEN 'CA'
        WHEN toUpper(text) IN ['COLORADO', 'CO'] THEN 'CO'
        WHEN toUpper(text) IN ['CONNECTICUT', 'CT'] THEN 'CT'
        WHEN toUpper(text) IN ['DELAWARE', 'DE'] THEN 'DE'
        WHEN toUpper(text) IN ['FLORIDA', 'FL'] THEN 'FL'
        WHEN toUpper(text) IN ['GEORGIA', 'GA'] THEN 'GA'
        WHEN toUpper(text) IN ['HAWAII', 'HI'] THEN 'HI'
        WHEN toUpper(text) IN ['IDAHO', 'ID'] THEN 'ID'
        WHEN toUpper(text) IN ['ILLINOIS', 'IL'] THEN 'IL'
        WHEN toUpper(text) IN ['INDIANA', 'IN'] THEN 'IN'
        WHEN toUpper(text) IN ['IOWA', 'IA'] THEN 'IA'
        WHEN toUpper(text) IN ['KANSAS', 'KS'] THEN 'KS'
        WHEN toUpper(text) IN ['KENTUCKY', 'KY'] THEN 'KY'
        WHEN toUpper(text) IN ['LOUISIANA', 'LA'] THEN 'LA'
        WHEN toUpper(text) IN ['MAINE', 'ME'] THEN 'ME'
        WHEN toUpper(text) IN ['MARYLAND', 'MD'] THEN 'MD'
        WHEN toUpper(text) IN ['MASSACHUSETTS', 'MA'] THEN 'MA'
        WHEN toUpper(text) IN ['MICHIGAN', 'MI'] THEN 'MI'
        WHEN toUpper(text) IN ['MINNESOTA', 'MN'] THEN 'MN'
        WHEN toUpper(text) IN ['MISSISSIPPI', 'MS'] THEN 'MS'
        WHEN toUpper(text) IN ['MISSOURI', 'MO'] THEN 'MO'
        WHEN toUpper(text) IN ['MONTANA', 'MT'] THEN 'MT'
        WHEN toUpper(text) IN ['NEBRASKA', 'NE'] THEN 'NE'
        WHEN toUpper(text) IN ['NEVADA', 'NV'] THEN 'NV'
        WHEN toUpper(text) IN ['NEW HAMPSHIRE', 'NH'] THEN 'NH'
        WHEN toUpper(text) IN ['NEW JERSEY', 'NJ'] THEN 'NJ'
        WHEN toUpper(text) IN ['NEW MEXICO', 'NM'] THEN 'NM'
        WHEN toUpper(text) IN ['NEW YORK', 'NY'] THEN 'NY'
        WHEN toUpper(text) IN ['NORTH CAROLINA', 'NC'] THEN 'NC'
        WHEN toUpper(text) IN ['NORTH DAKOTA', 'ND'] THEN 'ND'
        WHEN toUpper(text) IN ['OHIO', 'OH'] THEN 'OH'
        WHEN toUpper(text) IN ['OKLAHOMA', 'OK'] THEN 'OK'
        WHEN toUpper(text) IN ['OREGON', 'OR'] THEN 'OR'
        WHEN toUpper(text) IN ['PENNSYLVANIA', 'PA'] THEN 'PA'
        WHEN toUpper(text) IN ['RHODE ISLAND', 'RI'] THEN 'RI'
        WHEN toUpper(text) IN ['SOUTH CAROLINA', 'SC'] THEN 'SC'
        WHEN toUpper(text) IN ['SOUTH DAKOTA', 'SD'] THEN 'SD'
        WHEN toUpper(text) IN ['TENNESSEE', 'TN'] THEN 'TN'
        WHEN toUpper(text) IN ['TEXAS', 'TX'] THEN 'TX'
        WHEN toUpper(text) IN ['UTAH', 'UT'] THEN 'UT'
        WHEN toUpper(text) IN ['VERMONT', 'VT'] THEN 'VT'
        WHEN toUpper(text) IN ['VIRGINIA', 'VA'] THEN 'VA'
        WHEN toUpper(text) IN ['WASHINGTON', 'WA'] THEN 'WA'
        WHEN toUpper(text) IN ['WEST VIRGINIA', 'WV'] THEN 'WV'
        WHEN toUpper(text) IN ['WISCONSIN', 'WI'] THEN 'WI'
        WHEN toUpper(text) IN ['WYOMING', 'WY'] THEN 'WY'
        WHEN toUpper(text) IN ['DISTRICT OF COLUMBIA', 'DC'] THEN 'DC'
        WHEN size(text) = 2 THEN toUpper(text)
        ELSE 'XX'
    END";

// Country with transformation
MATCH (s:SourceField {name: "country"})
MATCH (t:TargetField {name: "land1"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "CASE 
        WHEN toUpper(text) IN ['USA', 'US'] THEN 'USA'
        ELSE toUpper(text)
    END";

// Payment terms with transformation
MATCH (s:SourceField {name: "payment terms"})
MATCH (t:TargetField {name: "zterm"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
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

// Tax ID with transformation
MATCH (s:SourceField {name: "taxid"})
MATCH (t:TargetField {name: "stcd1"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "CASE 
        WHEN NOT text CONTAINS '-' AND size(text) >= 7 
        THEN substring(text, 0, 2) + '-' + substring(text, 2)
        ELSE text
    END";

// Remaining direct mappings
MATCH (s:SourceField {name: "credit_limit"})
MATCH (t:TargetField {name: "klimk"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "credit rating"})
MATCH (t:TargetField {name: "kdgrp"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "type"})
MATCH (t:TargetField {name: "ktokd"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "sale org"})
MATCH (t:TargetField {name: "vkorg"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "channel"})
MATCH (t:TargetField {name: "vtweg"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

MATCH (s:SourceField {name: "division"})
MATCH (t:TargetField {name: "spart"})
MERGE (s)-[:MAPPED_TO {rule: "direct"}]->(t);

// Update column order based on the mappings sequence
MERGE (c:ColumnOrder {name: "default"})
SET c.order = [
    "kunnr",
    "name1",
    "stras",
    "ort01",
    "regio",
    "land1",
    "zterm",
    "stcd1",
    "klimk",
    "kdgrp",
    "ktokd",
    "vkorg",
    "vtweg",
    "spart"
];