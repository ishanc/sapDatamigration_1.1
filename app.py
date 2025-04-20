import os
import csv
from neo4j import GraphDatabase
from dotenv import load_dotenv
import pandas as pd
from config import CONFIG

# Load environment variables
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")

class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def fetch_transformation_rules(self):
        query = """
        MATCH (s:SourceField)-[r:MAPPED_TO]->(t:TargetField)
        RETURN s.name AS source_field, t.name AS target_field, r.rule AS mapping_rule, 
               r.transform_query AS transform_query, t.type AS target_type
        """
        with self.driver.session(database=NEO4J_DATABASE) as session:
            result = session.run(query)
            return [record for record in result]

    def fetch_column_standardization_rules(self):
        query = """
        MATCH (cs:ColumnStandardization {name: 'field_standardization'})
        RETURN cs.rules as rules
        """
        with self.driver.session(database=NEO4J_DATABASE) as session:
            result = session.run(query)
            record = result.single()
            if record and record["rules"]:
                import json
                return json.loads(record["rules"])
            return []

    def apply_transformation(self, value, rule, transform_query=None):
        if rule == "direct" or not rule:
            return value
            
        if rule == "transform" and transform_query:
            transform_query = transform_query.strip()
            query = f"""
            WITH $value AS text
            RETURN {transform_query} AS result
            """
            with self.driver.session(database=NEO4J_DATABASE) as session:
                try:
                    result = session.run(query, value=value)
                    record = result.single()
                    return record["result"] if record else value
                except Exception as e:
                    print(f"Error applying transformation: {str(e)}")
                    return value
        return value

    def fetch_column_order(self):
        query = """
        MATCH (c:ColumnOrder {name: "default"})
        RETURN c.order AS column_order
        """
        with self.driver.session(database=NEO4J_DATABASE) as session:
            result = session.run(query)
            record = result.single()
            return record["column_order"] if record else []

def get_source_target_mappings(mapping_file):
    """Read source-target mappings from mapping file"""
    mappings = {}
    with open(mapping_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mappings[row['SourceField']] = {
                'target': row['TargetField'],
                'rule': row['MappingRule']
            }
    return mappings

def process_dataframes(config):
    """Process and merge dataframes based on dynamic configuration"""
    dataframes = {}
    source_mappings = get_source_target_mappings(config['mapping_file'])
    source_fields = list(source_mappings.keys())
    
    # Initialize Neo4j handler
    neo4j_handler = Neo4jHandler(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Get standardization rules from Neo4j
        standardization_rules = neo4j_handler.fetch_column_standardization_rules()
        
        # Load each dataframe
        for key, file_config in config['files_config'].items():
            df = pd.read_csv(file_config['path'])
            
            # Clean column names - strip whitespace
            df.columns = df.columns.str.strip()
            
            # First standardize the key field if needed
            if file_config['key_field'] == 'index':
                df = df.rename(columns={'index': 'customercode'})
            
            # Apply other standardization rules
            for rule in standardization_rules:
                if rule['source_field'] in df.columns and rule['source_field'] != 'index':
                    # Check if condition exists and is satisfied
                    if rule['condition'] is None:
                        df = df.rename(columns={rule['source_field']: rule['target_field']})
                    else:
                        # Create a safe evaluation context
                        eval_context = {'key_field': file_config['key_field']}
                        try:
                            if eval(rule['condition'], {"__builtins__": {}}, eval_context):
                                df = df.rename(columns={rule['source_field']: rule['target_field']})
                        except Exception as e:
                            print(f"Warning: Failed to evaluate condition '{rule['condition']}': {str(e)}")
                
            # Keep only mapped source fields plus the key field
            keep_columns = [col for col in df.columns if col in ['customercode'] + source_fields]
            df = df[keep_columns]
                
            # Keep original dataframe
            dataframes[key] = df
    finally:
        neo4j_handler.close()

    # First merge all non-finance files
    base_files = [k for k in config['merge_order'] if k in dataframes and 'finance' not in k]
    if base_files:
        result_df = dataframes[base_files[0]]
        for key in base_files[1:]:
            if key in dataframes:
                result_df = pd.merge(
                    result_df,
                    dataframes[key],
                    on='customercode',
                    how='outer'
                )
    else:
        result_df = pd.DataFrame()

    # Handle finance data files separately
    finance_files = sorted([k for k in dataframes.keys() if 'finance' in k])
    if finance_files:
        # Start with empty finance dataframe
        finance_df = pd.DataFrame()
        
        # Process each finance file in order (copy file last)
        for file_key in finance_files:
            df = dataframes[file_key]
            if finance_df.empty:
                finance_df = df
            else:
                # For each customer in the new file
                for idx, row in df.iterrows():
                    cust_code = row['customercode']
                    # Update existing or append new
                    if cust_code in finance_df['customercode'].values:
                        mask = finance_df['customercode'] == cust_code
                        # Update non-null values
                        for col in df.columns:
                            if col != 'customercode' and pd.notna(row[col]):
                                finance_df.loc[mask, col] = row[col]
                    else:
                        finance_df = pd.concat([finance_df, pd.DataFrame([row])], ignore_index=True)

        # Merge finance data with base data
        if not result_df.empty:
            result_df = pd.merge(
                result_df,
                finance_df,
                on='customercode',
                how='outer',
                suffixes=('', '_finance')
            )
            
            # Clean up duplicate columns while preserving all values
            for col in result_df.columns:
                if col.endswith('_finance'):
                    base_col = col.replace('_finance', '')
                    if base_col in result_df.columns:
                        # For payment terms, prefer finance values
                        if base_col == 'payment terms':
                            mask = result_df[col].notna()
                            result_df.loc[mask, base_col] = result_df.loc[mask, col]
                        else:
                            # For other columns, only fill nulls
                            result_df[base_col] = result_df[base_col].fillna(result_df[col])
                    else:
                        result_df[base_col] = result_df[col]
                    result_df = result_df.drop(columns=[col])
        else:
            result_df = finance_df

    return result_df

def process_data():
    # Get source-target mappings
    mappings = get_source_target_mappings(CONFIG['mapping_file'])
    
    # Process dataframes
    merged_df = process_dataframes(CONFIG)

    # Initialize Neo4j handler
    neo4j_handler = Neo4jHandler(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Get transformation rules
        transformation_rules = neo4j_handler.fetch_transformation_rules()
        field_mappings = {rule['source_field']: rule for rule in transformation_rules}

        # Create output dataframe
        output_df = pd.DataFrame()
        
        # Process each mapping
        for source_field, mapping in mappings.items():
            target_field = mapping['target']
            if source_field in merged_df.columns:
                print(f"\nProcessing {source_field} -> {target_field}")
                
                if source_field in field_mappings:
                    rule = field_mappings[source_field]
                    if rule['mapping_rule'] == 'direct':
                        output_df[target_field] = merged_df[source_field].fillna('')
                    elif rule['mapping_rule'] == 'transform':
                        output_df[target_field] = merged_df[source_field].apply(
                            lambda x: neo4j_handler.apply_transformation(
                                x, 
                                rule['mapping_rule'], 
                                rule.get('transform_query')
                            ) if pd.notna(x) else ''
                        )
                else:
                    output_df[target_field] = merged_df[source_field].fillna('')

        # Get and apply column order
        column_order = neo4j_handler.fetch_column_order()
        if column_order:
            ordered_columns = [col for col in column_order if col in output_df.columns]
            output_df = output_df[ordered_columns]

        # Save output
        output_file = 'transformed_output.csv'
        output_df.to_csv(output_file, index=False)
        
        return {
            'status': 'success',
            'message': f'Transformation complete. Output written to {output_file}',
            'records': len(output_df),
            'columns': list(output_df.columns),
            'download_url': f'/download/{output_file}'
        }
        
    finally:
        neo4j_handler.close()

if __name__ == "__main__":
    result = process_data()
    print("\nTransformation Results:")
    print(f"Status: {result['status']}")
    print(f"Message: {result['message']}")
    print(f"Records processed: {result['records']}")
    print(f"Columns in output: {result['columns']}")