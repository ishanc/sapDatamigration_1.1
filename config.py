"""Configuration settings for SAP data migration"""
import os
import csv

def discover_csv_files(directory="uploads"):
    """Dynamically discover CSV files and their configurations"""
    files_config = {}
    
    # Skip if directory doesn't exist yet
    if not os.path.exists(directory):
        return files_config
    
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                reader = csv.reader(f)
                headers = next(reader)
                
                # Determine key field based on headers
                key_field = 'customercode' if 'customercode' in headers else 'index'
                
                # Create config entry
                config_key = filename.lower().replace('.csv', '').replace(' ', '_')
                files_config[config_key] = {
                    'path': filepath,
                    'key_field': key_field,
                    'headers': headers
                }
    
    return files_config

def get_merge_order(files_config):
    """Determine merge order based on file dependencies"""
    # Find source files that contain customer data
    customer_files = []
    for key, config in files_config.items():
        if any(header in config['headers'] for header in ['customercode', 'index']):
            customer_files.append(key)
    return customer_files

def load_config():
    """Load all configuration dynamically"""
    config = {
        'files_config': discover_csv_files(),  # Now uses uploads directory by default
        'mapping_file': os.path.join('originalFiles', 'SourceTargetCustomerMasterRelationship.csv'),
        'output_file': 'transformed_output.csv'
    }
    config['merge_order'] = get_merge_order(config['files_config'])
    return config

# Load configuration
CONFIG = load_config()