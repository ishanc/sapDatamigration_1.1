import os
from neo4j import GraphDatabase
from dotenv import load_dotenv, find_dotenv
from urllib.parse import urlparse

def validate_neo4j_setup():
    # Force reload environment variables
    load_dotenv(find_dotenv(), override=True)

    # Get and validate Neo4j connection details from environment
    uri = os.getenv("NEO4J_URI")
    print(f"Loaded URI from env: {uri}")
    
    if not uri:
        raise ValueError("NEO4J_URI environment variable is not set")

    # Validate URI scheme
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme not in ['bolt', 'neo4j', 'neo4j+s', 'neo4j+ssc']:
        raise ValueError(f"Invalid Neo4j URI scheme: {parsed_uri.scheme}")

    print(f"Using scheme: {parsed_uri.scheme}")
    print(f"Using hostname: {parsed_uri.hostname}")
    print(f"Using port: {parsed_uri.port}")

    # Connect to Neo4j using environment variables
    driver = GraphDatabase.driver(
        uri.strip(),
        auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
    )

    try:
        with driver.session() as session:
            # Check source fields
            result = session.run("""
                MATCH (s:SourceField)
                RETURN s.name as name, s.type as type, s.length as length
                ORDER BY s.name
            """)
            print("\nSource Fields:")
            print("-" * 50)
            for record in result:
                print(f"Name: {record['name']}, Type: {record['type']}, Length: {record['length']}")

            # Check target fields
            result = session.run("""
                MATCH (t:TargetField)
                RETURN t.name as name, t.type as type, t.length as length
                ORDER BY t.name
            """)
            print("\nTarget Fields:")
            print("-" * 50)
            for record in result:
                print(f"Name: {record['name']}, Type: {record['type']}, Length: {record['length']}")

            # Check mappings and transformations
            result = session.run("""
                MATCH (s:SourceField)-[r:MAPPED_TO]->(t:TargetField)
                RETURN s.name as source, t.name as target, r.rule as rule, r.transform_query as transform
                ORDER BY s.name
            """)
            print("\nMappings and Transformations:")
            print("-" * 50)
            for record in result:
                print(f"Source: {record['source']} -> Target: {record['target']}")
                print(f"Rule: {record['rule']}")
                if record['transform']:
                    print(f"Transform: {record['transform']}\n")

            # Check column order
            result = session.run("""
                MATCH (c:ColumnOrder {name: 'default'})
                RETURN c.order as order
            """)
            print("\nColumn Order:")
            print("-" * 50)
            record = result.single()
            if record and record['order']:
                print("Columns in order:")
                for col in record['order']:
                    print(f"- {col}")
            else:
                print("No column order defined")

    finally:
        driver.close()

if __name__ == "__main__":
    validate_neo4j_setup()