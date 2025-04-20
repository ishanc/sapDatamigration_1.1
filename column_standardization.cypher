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