import os
import csv
from neo4j import GraphDatabase
from dotenv import load_dotenv
from huggingface_hub import InferenceClient

# Load environment variables
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")

# Initialize Hugging Face client with the provided token
client = InferenceClient(
    provider="hf-inference",
    token="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"
)

def query_llm(prompt):
    """
    Query the Hugging Face LLM with a given prompt using streaming.
    :param prompt: The user input to send to the LLM.
    :return: The LLM's response as a string.
    """
    try:
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]

        stream = client.chat.completions.create(
            model="mistralai/Mistral-Nemo-Instruct-2407",
            messages=messages,
            max_tokens=500,
            stream=True
        )

        response = ""
        for chunk in stream:
            content = chunk.choices[0].delta.content
            if content:
                response += content
                print(content, end="")  # Print the streaming output
        
        return response

    except Exception as e:
        print(f"Error querying LLM: {e}")
        return "An error occurred while querying the LLM."

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
            
    def apply_transformation(self, value, rule, transform_query=None):
        """Execute a transformation rule in Neo4j"""
        if rule == "direct" or not rule:
            return value
            
        if rule == "transform" and transform_query:
            # Clean up the transform query by removing extra whitespace
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

# Initialize Neo4j handler
neo4j_handler = Neo4jHandler(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

# Fetch transformation rules
transformation_rules = neo4j_handler.fetch_transformation_rules()

# Close Neo4j connection
neo4j_handler.close()

# Apply transformation logic to source files
def transform_data(source_files, transformation_rules, output_file):
    # Dictionary to store consolidated data by customer code/index
    consolidated_data = {}
    
    # Create Neo4j handler for transformations
    neo4j_handler = Neo4jHandler(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    # Create Neo4j mappings dictionary with rules
    neo4j_mappings = {rule['source_field']: {'target': rule['target_field'], 'rule': rule['mapping_rule'], 'transform_query': rule.get('transform_query')} 
                     for rule in transformation_rules}
    
    # Process each source file
    for source_file in source_files:
        try:
            print(f"\nProcessing file: {source_file}")
            with open(source_file, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                fieldnames = [name.strip() for name in reader.fieldnames]
                print(f"Fields found: {fieldnames}")
                
                for row_num, row in enumerate(reader, 1):
                    clean_row = {k.strip(): v.strip() if v else "" for k, v in row.items()}
                    key = (clean_row.get('customercode', '') or clean_row.get('index', ''))
                    
                    if key:
                        if key not in consolidated_data:
                            consolidated_data[key] = {}
                        
                        # Store values directly with their source field names
                        for csv_field, value in clean_row.items():
                            if value:
                                consolidated_data[key][csv_field.strip()] = value
                                print(f"Stored value for {csv_field}: {value}")
                    else:
                        print(f"Warning: No valid key found for row {row_num}")

        except Exception as e:
            print(f"Error processing file {source_file}: {str(e)}")
            continue

    # Transform consolidated data using Neo4j mappings
    transformed_data = []
    print(f"\nNumber of records consolidated: {len(consolidated_data)}")
    
    for key, data in consolidated_data.items():
        transformed_row = {}
        # Apply Neo4j mappings to get final target fields
        for source_field, value in data.items():
            if source_field in neo4j_mappings:
                mapping = neo4j_mappings[source_field]
                target_field = mapping['target']
                transformed_row[target_field] = neo4j_handler.apply_transformation(value, mapping['rule'], mapping.get('transform_query'))
                print(f"Mapped {source_field} -> {target_field}: {value} -> {transformed_row[target_field]}")
        
        if transformed_row:
            transformed_data.append(transformed_row)

    # Close Neo4j connection
    neo4j_handler.close()

    # Write transformed data to output file
    try:
        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            if transformed_data:
                fieldnames = set()
                for row in transformed_data:
                    fieldnames.update(row.keys())
                
                writer = csv.DictWriter(file, fieldnames=sorted(list(fieldnames)))
                writer.writeheader()
                writer.writerows(transformed_data)
                print(f"\nSuccessfully wrote {len(transformed_data)} records to {output_file}")
                print(f"Fields written: {sorted(list(fieldnames))}")
            else:
                print("No data to write to output file")
    except Exception as e:
        print(f"Error writing to output file {output_file}: {str(e)}")

# Define source files and output file
source_files = [
    os.path.join("originalFiles", "customer address.csv"),
    os.path.join("originalFiles", "customer finances.csv"),
    os.path.join("originalFiles", "customer type.csv")
]
output_file = "transformed_output.csv"

# Transform data
transform_data(source_files, transformation_rules, output_file)

print(f"Transformation complete. Output written to {output_file}.")

# Example usage of LLM for resolving mismatches
mismatch_example = "The source field 'customername 1' does not match the target field 'name1'."
response = query_llm(mismatch_example)
print(f"LLM Suggestion: {response}")

# Example usage
if __name__ == "__main__":
    user_prompt = "hi"
    response = query_llm(user_prompt)
    print(response)