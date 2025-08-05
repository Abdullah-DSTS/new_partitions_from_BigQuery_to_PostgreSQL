"""
GCP Clients initialization module for BigQuery to PostgreSQL transfer
"""
from google.cloud import bigquery
from google.cloud import storage
import yaml
import os


class GCPClients:
    def __init__(self, config_path="config/config.yaml"):
        self.config = self._load_config(config_path)
        self.bq_client = None
        self.gcs_client = None
    
    def _load_config(self, config_path):
        """Load configuration from YAML file"""
        try:
            # Resolve relative path to absolute path
            if not os.path.isabs(config_path):
                config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), config_path)
            
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file: {e}")
    
    def initialize_bigquery_client(self):
        """Initialize BigQuery client with service account"""
        try:
            credentials_path = self.config['bigquery']['service_account_path']
            
            # Resolve relative path to absolute path
            if not os.path.isabs(credentials_path):
                credentials_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), credentials_path)
            
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"BigQuery service account file not found: {credentials_path}")
            
            self.bq_client = bigquery.Client.from_service_account_json(credentials_path)
            return self.bq_client
        except Exception as e:
            raise Exception(f"Failed to initialize BigQuery client: {e}")
    
    def initialize_gcs_client(self):
        """Initialize GCS client with service account"""
        try:
            credentials_path = self.config['gcs']['service_account_path']
            
            # Resolve relative path to absolute path
            if not os.path.isabs(credentials_path):
                credentials_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), credentials_path)
            
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"GCS service account file not found: {credentials_path}")
            
            self.gcs_client = storage.Client.from_service_account_json(credentials_path)
            return self.gcs_client
        except Exception as e:
            raise Exception(f"Failed to initialize GCS client: {e}")
    
    def get_clients(self):
        """Get both clients, initializing if needed"""
        if not self.bq_client:
            self.initialize_bigquery_client()
        if not self.gcs_client:
            self.initialize_gcs_client()
        return self.bq_client, self.gcs_client
    
    def get_bigquery_client(self):
        """Get BigQuery client only"""
        if not self.bq_client:
            self.initialize_bigquery_client()
        return self.bq_client
    
    def get_gcs_client(self):
        """Get GCS client only"""
        if not self.gcs_client:
            self.initialize_gcs_client()
        return self.gcs_client
    
    def get_config(self):
        """Get the loaded configuration"""
        return self.config


# Singleton pattern for global client access
_gcp_clients_instance = None

def get_gcp_clients(config_path="config/config.yaml"):
    """Get singleton instance of GCPClients"""
    global _gcp_clients_instance
    if _gcp_clients_instance is None:
        _gcp_clients_instance = GCPClients(config_path)
    return _gcp_clients_instance 