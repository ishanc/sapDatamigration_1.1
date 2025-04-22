from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get Neo4j credentials from environment
uri = os.getenv("NEO4J_URI")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASSWORD")

def run_setup():
    # Read the cypher script
    with open('complete_setup.cypher', 'r') as file:
        setup_script = file.read()
    
    # Connect to Neo4j
    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    try:
        with driver.session() as session:
            print("Executing setup script...")
            # Split the script into individual statements
            statements = setup_script.split(';')
            
            for statement in statements:
                # Skip empty statements
                if statement.strip():
                    try:
                        print(f"\nExecuting statement:")
                        print(statement.strip())
                        result = session.run(statement)
                        print("Statement executed successfully")
                    except Exception as e:
                        print(f"Error executing statement: {e}")
            
            print("\nSetup complete!")
    
    finally:
        driver.close()

if __name__ == "__main__":
    run_setup()