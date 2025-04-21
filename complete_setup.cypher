// First clean up everything in the database
MATCH (n) DETACH DELETE n;

// Create Source Field Nodes
CREATE (s1:SourceField {name: "customercode"})
SET s1.type = "char", s1.length = 10;

CREATE (s2:SourceField {name: "name"})
SET s2.type = "char", s2.length = 50;

CREATE (s3:SourceField {name: "address"})
SET s3.type = "char", s3.length = 45;

CREATE (s4:SourceField {name: "city"})
SET s4.type = "char", s4.length = 35;

CREATE (s5:SourceField {name: "state"})
SET s5.type = "char", s5.length = 15;

CREATE (s6:SourceField {name: "country"})
SET s6.type = "char", s6.length = 3;

CREATE (s7:SourceField {name: "payment terms"})
SET s7.type = "char", s7.length = 4;

CREATE (s8:SourceField {name: "taxid"})
SET s8.type = "char", s8.length = 16;

CREATE (s9:SourceField {name: "credit_limit"})
SET s9.type = "char", s9.length = 18;

CREATE (s10:SourceField {name: "credit rating"})
SET s10.type = "char", s10.length = 2;

CREATE (s11:SourceField {name: "type"})
SET s11.type = "char", s11.length = 4;

CREATE (s12:SourceField {name: "sale org"})
SET s12.type = "char", s12.length = 4;

CREATE (s13:SourceField {name: "channel"})
SET s13.type = "char", s13.length = 2;

CREATE (s14:SourceField {name: "division"})
SET s14.type = "char", s14.length = 4;

// Create Target Field Nodes with proper attributes
CREATE (t1:TargetField {name: "kunnr"})
SET t1.type = "char", t1.length = 10;

CREATE (t2:TargetField {name: "name1"})
SET t2.type = "char", t2.length = 35;

CREATE (t3:TargetField {name: "stras"})
SET t3.type = "char", t3.length = 35;

CREATE (t4:TargetField {name: "ort01"})
SET t4.type = "char", t4.length = 35;

CREATE (t5:TargetField {name: "regio"})
SET t5.type = "char", t5.length = 3;

CREATE (t6:TargetField {name: "land1"})
SET t6.type = "char", t6.length = 3;

CREATE (t7:TargetField {name: "zterm"})
SET t7.type = "char", t7.length = 4;

CREATE (t8:TargetField {name: "stcd1"})
SET t8.type = "char", t8.length = 16;

CREATE (t9:TargetField {name: "klimk"})
SET t9.type = "char", t9.length = 18;

CREATE (t10:TargetField {name: "kdgrp"})
SET t10.type = "char", t10.length = 2;

CREATE (t11:TargetField {name: "ktokd"})
SET t11.type = "char", t11.length = 4;

CREATE (t12:TargetField {name: "vkorg"})
SET t12.type = "char", t12.length = 4;

CREATE (t13:TargetField {name: "vtweg"})
SET t13.type = "char", t13.length = 2;

CREATE (t14:TargetField {name: "spart"})
SET t14.type = "char", t14.length = 4;

// Create direct mappings
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

// Create transformation mappings
// State transformation with comprehensive US state mapping
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
        WHEN text IS NULL OR text = '' THEN ''
        WHEN size(text) = 2 THEN toUpper(text)
        ELSE 'XX'
    END";

// Country transformation with better null handling
MATCH (s:SourceField {name: "country"})
MATCH (t:TargetField {name: "land1"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "CASE 
        WHEN text IS NULL OR text = '' THEN ''
        WHEN toUpper(text) IN ['USA', 'US', 'UNITED STATES', 'UNITED STATES OF AMERICA'] THEN 'USA'
        ELSE toUpper(trim(text))
    END";

// Payment terms transformation with improved numeric handling
MATCH (s:SourceField {name: "payment terms"})
MATCH (t:TargetField {name: "zterm"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "CASE 
        WHEN text IS NULL OR text = '' THEN ''
        WHEN toLower(text) CONTAINS 'immediate' THEN 'T000'
        WHEN text = '0' OR toLower(text) CONTAINS 'net 0' THEN 'T001'
        WHEN toLower(text) STARTS WITH 'net ' THEN 
            CASE
                WHEN size(replace(substring(text, 4), ' ', '')) = 1 
                THEN 'T00' + replace(substring(text, 4), ' ', '')
                WHEN size(replace(substring(text, 4), ' ', '')) = 2 
                THEN 'T0' + replace(substring(text, 4), ' ', '')
                WHEN size(replace(substring(text, 4), ' ', '')) = 3
                THEN 'T' + replace(substring(text, 4), ' ', '')
                ELSE text
            END
        WHEN size(replace(text, ' ', '')) = 1 THEN 'T00' + replace(text, ' ', '')
        WHEN size(replace(text, ' ', '')) = 2 THEN 'T0' + replace(text, ' ', '')
        WHEN size(replace(text, ' ', '')) = 3 THEN 'T' + replace(text, ' ', '')
        ELSE 'T' + text
    END";

// Tax ID transformation with better validation
MATCH (s:SourceField {name: "taxid"})
MATCH (t:TargetField {name: "stcd1"})
MERGE (s)-[r:MAPPED_TO]->(t)
SET r.rule = "transform",
    r.transform_query = "CASE 
        WHEN text IS NULL OR text = '' THEN ''
        WHEN NOT text CONTAINS '-' AND size(text) >= 7 
        THEN substring(text, 0, 2) + '-' + substring(text, 2)
        ELSE trim(text)
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

// Create column standardization rules
MERGE (cs:ColumnStandardization {name: "field_standardization"})
SET cs.rules = '[
    {
        "source_field": "index",
        "target_field": "customercode",
        "condition": "key_field == \'index\'"
    },
    {
        "source_field": "pay_terms",
        "target_field": "payment terms",
        "condition": null
    }
]';

// Set up final column order
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