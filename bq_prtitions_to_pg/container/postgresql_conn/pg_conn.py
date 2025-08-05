"""
PostgreSQL Connection module for BigQuery to PostgreSQL transfer
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import yaml
import os


class PostgreSQLConnection:
    def __init__(self, config_path="config/config.yaml"):
        self.config = self._load_config(config_path)
        self.pg_connection = None
    
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
    
    def initialize_postgresql_connection(self):
        """Initialize PostgreSQL connection"""
        try:
            pg_config = self.config['postgresql']
            connection_params = {
                'host': pg_config['host'],
                'port': pg_config['port'],
                'database': pg_config['database'],
                'user': pg_config['username'],
                'password': pg_config['password']
            }
            
            # Validate required parameters
            for key, value in connection_params.items():
                if not value:
                    raise ValueError(f"PostgreSQL {key} is not configured")
            
            self.pg_connection = psycopg2.connect(**connection_params)
            return self.pg_connection
        except Exception as e:
            raise Exception(f"Failed to initialize PostgreSQL connection: {e}")
    
    def get_postgresql_connection(self):
        """Get PostgreSQL connection, initializing if needed"""
        if not self.pg_connection or self.pg_connection.closed:
            self.initialize_postgresql_connection()
        return self.pg_connection
    
    def get_postgresql_cursor(self, dict_cursor=True):
        """Get PostgreSQL cursor with optional dictionary cursor"""
        connection = self.get_postgresql_connection()
        if dict_cursor:
            return connection.cursor(cursor_factory=RealDictCursor)
        return connection.cursor()
    
    def close_postgresql_connection(self):
        """Close PostgreSQL connection"""
        if self.pg_connection and not self.pg_connection.closed:
            self.pg_connection.close()
            self.pg_connection = None
    
    def get_config(self):
        """Get the loaded configuration"""
        return self.config
    
    def test_connection(self):
        """Test PostgreSQL connection"""
        try:
            cursor = self.get_postgresql_cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            print(f"PostgreSQL connection test failed: {e}")
            return False


# Singleton pattern for global client access
_postgresql_connection_instance = None

def get_postgresql_connection(config_path="config/config.yaml"):
    """Get singleton instance of PostgreSQLConnection"""
    global _postgresql_connection_instance
    if _postgresql_connection_instance is None:
        _postgresql_connection_instance = PostgreSQLConnection(config_path)
    return _postgresql_connection_instance 