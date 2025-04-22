import os
import csv
from neo4j import GraphDatabase
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from config import CONFIG
from urllib.parse import urlparse

# Force reload environment variables
load_dotenv(find_dotenv(), override=True)

# Get and validate Neo4j connection details from environment
NEO4J_URI = os.getenv("NEO4J_URI")
if not NEO4J_URI:
    raise ValueError("NEO4J_URI environment variable is not set")

# Validate URI scheme
parsed_uri = urlparse(NEO4J_URI)
if (parsed_uri.scheme not in ['bolt', 'neo4j', 'neo4j+s', 'neo4j+ssc']):
    raise ValueError(f"Invalid Neo4j URI scheme: {parsed_uri.scheme}")

NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")  # Will be None if not in .env

class Neo4jHandler:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri.strip(), auth=(user, password))
        # Only use database from environment if explicitly set
        self.database = os.getenv("NEO4J_DATABASE") if os.getenv("NEO4J_DATABASE") else None

    def close(self):
        self.driver.close()

    def _get_session(self):
        # Only pass database parameter if explicitly set
        return self.driver.session(database=self.database) if self.database else self.driver.session()

    def fetch_transformation_rules(self):
        query = """
        MATCH (s:SourceField)-[r:MAPPED_TO]->(t:TargetField)
        RETURN s.name AS source_field, t.name AS target_field, r.rule AS mapping_rule, 
               r.transform_query AS transform_query, t.type AS target_type
        """
        with self._get_session() as session:
            result = session.run(query)
            return [record for record in result]

    def fetch_column_standardization_rules(self):
        query = """
        MATCH (cs:ColumnStandardization {name: 'field_standardization'})
        RETURN cs.rules as rules
        """
        with self._get_session() as session:
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
            with self._get_session() as session:
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
        with self._get_session() as session:
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
        
        # Load each dataframe only if file exists
        for key, file_config in config['files_config'].items():
            if not os.path.exists(file_config['path']):
                print(f"Warning: File {file_config['path']} does not exist, skipping")
                continue
                
            df = pd.read_csv(file_config['path'])
            
            # Clean column names - strip whitespace
            df.columns = df.columns.str.strip()
            
            # First standardize the key field if needed
            if file_config['key_field'] == 'index':
                df = df.rename(columns={'index': 'customercode'})
            
            # Apply other standardization rules
            for rule in standardization_rules:
                if rule['source_field'] in df.columns and rule['source_field'] != 'index':
                    if rule['condition'] is None:
                        df = df.rename(columns={rule['source_field']: rule['target_field']})
                    else:
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
                # For each file, handle duplicate columns before merging
                df_to_merge = dataframes[key]
                duplicate_cols = set(result_df.columns) & set(df_to_merge.columns) - {'customercode'}
                
                # For duplicate columns, keep the non-null values from both dataframes
                for col in duplicate_cols:
                    temp_col = f"{col}_temp"
                    df_to_merge = df_to_merge.rename(columns={col: temp_col})
                
                # Merge with temporary column names
                result_df = pd.merge(
                    result_df,
                    df_to_merge,
                    on='customercode',
                    how='outer'
                )
                
                # Combine values for duplicate columns
                for col in duplicate_cols:
                    temp_col = f"{col}_temp"
                    if temp_col in result_df.columns:
                        result_df[col] = result_df[col].fillna(result_df[temp_col])
                        result_df = result_df.drop(columns=[temp_col])
    else:
        result_df = pd.DataFrame()

    # Handle finance data files separately
    finance_files = sorted([k for k in dataframes.keys() if 'finance' in k])
    if finance_files:
        # Start with empty finance dataframe
        finance_df = pd.DataFrame()
        
        # Process each finance file in order
        for file_key in finance_files:
            df = dataframes[file_key]
            if finance_df.empty:
                finance_df = df
            else:
                # Handle duplicate columns before merging
                duplicate_cols = set(finance_df.columns) & set(df.columns) - {'customercode'}
                
                # For duplicate columns, keep the non-null values from both dataframes
                for col in duplicate_cols:
                    temp_col = f"{col}_temp"
                    df = df.rename(columns={col: temp_col})
                
                # Merge with temporary column names
                finance_df = pd.merge(
                    finance_df,
                    df,
                    on='customercode',
                    how='outer'
                )
                
                # Combine values for duplicate columns
                for col in duplicate_cols:
                    temp_col = f"{col}_temp"
                    if temp_col in finance_df.columns:
                        finance_df[col] = finance_df[col].fillna(finance_df[temp_col])
                        finance_df = finance_df.drop(columns=[temp_col])

        # Merge finance data with base data
        if not result_df.empty:
            # Handle duplicate columns before final merge
            duplicate_cols = set(result_df.columns) & set(finance_df.columns) - {'customercode'}
            
            # For duplicate columns, keep the non-null values from both dataframes
            for col in duplicate_cols:
                temp_col = f"{col}_temp"
                finance_df = finance_df.rename(columns={col: temp_col})
            
            # Merge with temporary column names
            result_df = pd.merge(
                result_df,
                finance_df,
                on='customercode',
                how='outer'
            )
            
            # Combine values for duplicate columns, preferring finance data for certain fields
            for col in duplicate_cols:
                temp_col = f"{col}_temp"
                if temp_col in result_df.columns:
                    if col == 'payment terms':
                        # For payment terms, prefer finance values
                        mask = result_df[temp_col].notna()
                        result_df.loc[mask, col] = result_df.loc[mask, temp_col]
                    else:
                        # For other columns, only fill nulls
                        result_df[col] = result_df[col].fillna(result_df[temp_col])
                    result_df = result_df.drop(columns=[temp_col])
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
                    
                    # Special handling for payment terms to ensure proper string conversion
                    if target_field == 'zterm':
                        # Convert to string and clean up numeric values
                        merged_df[source_field] = merged_df[source_field].fillna('')
                        merged_df[source_field] = merged_df[source_field].astype(str)
                        # Remove .0 from float strings
                        merged_df[source_field] = merged_df[source_field].replace(r'\.0$', '', regex=True)
                        
                    # Convert numeric types to string before transformation
                    elif merged_df[source_field].dtype in ['int64', 'float64']:
                        merged_df[source_field] = merged_df[source_field].fillna('')
                        merged_df[source_field] = merged_df[source_field].astype(str)
                        # Remove .0 from float strings for other numeric fields
                        merged_df[source_field] = merged_df[source_field].replace(r'\.0$', '', regex=True)
                    else:
                        # For non-numeric fields, just fill NaN with empty string
                        merged_df[source_field] = merged_df[source_field].fillna('')
                    
                    if rule['mapping_rule'] == 'direct':
                        output_df[target_field] = merged_df[source_field]
                    elif rule['mapping_rule'] == 'transform':
                        # Apply transformation to non-empty values
                        output_df[target_field] = merged_df[source_field].apply(
                            lambda x: neo4j_handler.apply_transformation(
                                str(x), 
                                rule['mapping_rule'], 
                                rule.get('transform_query')
                            ) if x != '' else ''
                        )
                else:
                    output_df[target_field] = merged_df[source_field].fillna('')

        # Final cleanup of any remaining NaN values
        output_df = output_df.fillna('')

        # Get and apply column order
        column_order = neo4j_handler.fetch_column_order()
        if (column_order):
            ordered_columns = [col for col in column_order if col in output_df.columns]
            output_df = output_df[ordered_columns]

        # Final cleanup of any remaining .0 in payment terms
        if 'zterm' in output_df.columns:
            output_df['zterm'] = output_df['zterm'].replace(r'\.0$', '', regex=True)

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

def add_state_transformation(uri, user, password):
    cypher = '''
    MATCH (s:SourceField {name: "state"})
    MATCH (t:TargetField {name: "regio"})
    MERGE (s)-[r:MAPPED_TO]->(t)
    SET r.rule = "transform",
        r.transform_query = "CASE \
            WHEN toUpper(text) IN ['ALABAMA', 'AL'] THEN 'AL' \
            WHEN toUpper(text) IN ['ALASKA', 'AK'] THEN 'AK' \
            WHEN toUpper(text) IN ['ARIZONA', 'AZ'] THEN 'AZ' \
            WHEN toUpper(text) IN ['ARKANSAS', 'AR'] THEN 'AR' \
            WHEN toUpper(text) IN ['CALIFORNIA', 'CA'] THEN 'CA' \
            WHEN toUpper(text) IN ['COLORADO', 'CO'] THEN 'CO' \
            WHEN toUpper(text) IN ['CONNECTICUT', 'CT'] THEN 'CT' \
            WHEN toUpper(text) IN ['DELAWARE', 'DE'] THEN 'DE' \
            WHEN toUpper(text) IN ['FLORIDA', 'FL'] THEN 'FL' \
            WHEN toUpper(text) IN ['GEORGIA', 'GA'] THEN 'GA' \
            WHEN toUpper(text) IN ['HAWAII', 'HI'] THEN 'HI' \
            WHEN toUpper(text) IN ['IDAHO', 'ID'] THEN 'ID' \
            WHEN toUpper(text) IN ['ILLINOIS', 'IL'] THEN 'IL' \
            WHEN toUpper(text) IN ['INDIANA', 'IN'] THEN 'IN' \
            WHEN toUpper(text) IN ['IOWA', 'IA'] THEN 'IA' \
            WHEN toUpper(text) IN ['KANSAS', 'KS'] THEN 'KS' \
            WHEN toUpper(text) IN ['KENTUCKY', 'KY'] THEN 'KY' \
            WHEN toUpper(text) IN ['LOUISIANA', 'LA'] THEN 'LA' \
            WHEN toUpper(text) IN ['MAINE', 'ME'] THEN 'ME' \
            WHEN toUpper(text) IN ['MARYLAND', 'MD'] THEN 'MD' \
            WHEN toUpper(text) IN ['MASSACHUSETTS', 'MA'] THEN 'MA' \
            WHEN toUpper(text) IN ['MICHIGAN', 'MI'] THEN 'MI' \
            WHEN toUpper(text) IN ['MINNESOTA', 'MN'] THEN 'MN' \
            WHEN toUpper(text) IN ['MISSISSIPPI', 'MS'] THEN 'MS' \
            WHEN toUpper(text) IN ['MISSOURI', 'MO'] THEN 'MO' \
            WHEN toUpper(text) IN ['MONTANA', 'MT'] THEN 'MT' \
            WHEN toUpper(text) IN ['NEBRASKA', 'NE'] THEN 'NE' \
            WHEN toUpper(text) IN ['NEVADA', 'NV'] THEN 'NV' \
            WHEN toUpper(text) IN ['NEW HAMPSHIRE', 'NH'] THEN 'NH' \
            WHEN toUpper(text) IN ['NEW JERSEY', 'NJ'] THEN 'NJ' \
            WHEN toUpper(text) IN ['NEW MEXICO', 'NM'] THEN 'NM' \
            WHEN toUpper(text) IN ['NEW YORK', 'NY'] THEN 'NY' \
            WHEN toUpper(text) IN ['NORTH CAROLINA', 'NC'] THEN 'NC' \
            WHEN toUpper(text) IN ['NORTH DAKOTA', 'ND'] THEN 'ND' \
            WHEN toUpper(text) IN ['OHIO', 'OH'] THEN 'OH' \
            WHEN toUpper(text) IN ['OKLAHOMA', 'OK'] THEN 'OK' \
            WHEN toUpper(text) IN ['OREGON', 'OR'] THEN 'OR' \
            WHEN toUpper(text) IN ['PENNSYLVANIA', 'PA'] THEN 'PA' \
            WHEN toUpper(text) IN ['RHODE ISLAND', 'RI'] THEN 'RI' \
            WHEN toUpper(text) IN ['SOUTH CAROLINA', 'SC'] THEN 'SC' \
            WHEN toUpper(text) IN ['SOUTH DAKOTA', 'SD'] THEN 'SD' \
            WHEN toUpper(text) IN ['TENNESSEE', 'TN'] THEN 'TN' \
            WHEN toUpper(text) IN ['TEXAS', 'TX'] THEN 'TX' \
            WHEN toUpper(text) IN ['UTAH', 'UT'] THEN 'UT' \
            WHEN toUpper(text) IN ['VERMONT', 'VT'] THEN 'VT' \
            WHEN toUpper(text) IN ['VIRGINIA', 'VA'] THEN 'VA' \
            WHEN toUpper(text) IN ['WASHINGTON', 'WA'] THEN 'WA' \
            WHEN toUpper(text) IN ['WEST VIRGINIA', 'WV'] THEN 'WV' \
            WHEN toUpper(text) IN ['WISCONSIN', 'WI'] THEN 'WI' \
            WHEN toUpper(text) IN ['WYOMING', 'WY'] THEN 'WY' \
            WHEN toUpper(text) IN ['DISTRICT OF COLUMBIA', 'DC'] THEN 'DC' \
            WHEN text IS NULL OR text = '' THEN '' \
            WHEN size(text) = 2 THEN toUpper(text) \
            ELSE 'XX' \
        END"
    '''
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            session.run(cypher)
            return True

def delete_state_transformation(uri, user, password):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            # First operation: Delete the existing relationship
            delete_query = "MATCH (:SourceField {name: 'state'})-[r:MAPPED_TO]->(:TargetField {name: 'regio'}) DELETE r"
            session.run(delete_query)
            
            # Second operation: Create new direct mapping
            create_query = "MATCH (s:SourceField {name: 'state'}), (t:TargetField {name: 'regio'}) CREATE (s)-[:MAPPED_TO {rule: 'direct'}]->(t)"
            session.run(create_query)
            
            return True

def state_rule_status(uri, user, password):
    cypher = '''
    MATCH (s:SourceField {name: "state"})-[r:MAPPED_TO]->(t:TargetField {name: "regio"})
    WHERE r.rule = "transform"
    RETURN COUNT(r) AS rule_count
    '''
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session() as session:
            result = session.run(cypher)
            count = result.single()["rule_count"]
            return count > 0

if __name__ == "__main__":
    result = process_data()
    print("\nTransformation Results:")
    print(f"Status: {result['status']}")
    print(f"Message: {result['message']}")
    print(f"Records processed: {result['records']}")
    print(f"Columns in output: {result['columns']}")