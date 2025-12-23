import yaml
import os
from pathlib import Path

def load_config(config_path=None):
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    env = os.getenv('ENVIRONMENT', config.get('environment', 'local'))
    
    if env == 'production':
        config['paths']['bronze'] = config['paths']['s3_bronze']
        config['paths']['silver'] = config['paths']['s3_silver']
        config['paths']['gold'] = config['paths']['s3_gold']
    
    return config

