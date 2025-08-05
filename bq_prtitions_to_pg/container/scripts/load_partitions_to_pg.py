"""
Script to load partitions from GCS processing_zone to PostgreSQL
"""
from google.cloud import storage
import pandas as pd
from io import BytesIO
import os
import sys
from datetime import datetime

# Add parent directory to path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gcp_clients.clients import get_gcp_clients
from postgresql_conn.pg_conn import get_postgresql_connection


def get_gcs_files_in_processing_zone(bucket_name, storage_client):
    """
    Get list of CSV files in processing_zone folder
    
    Args:
        bucket_name: GCS bucket name
        storage_client: GCS client instance
        
    Returns:
        list: List of file names in processing_zone
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix='processing_zone/')
        
        # Filter for CSV files only
        csv_files = [blob.name.replace('processing_zone/', '') for blob in blobs 
                    if blob.name.endswith('.csv') and blob.name.startswith('processing_zone/')]
        
        return csv_files
        
    except Exception as e:
        print(f"✗ Error getting files from processing_zone: {e}")
        return []


def move_file_in_gcs(bucket_name, storage_client, source_blob_name, destination_blob_name):
    """
    Move a file within GCS bucket
    
    Args:
        bucket_name: GCS bucket name
        storage_client: GCS client instance
        source_blob_name: Source blob name
        destination_blob_name: Destination blob name
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        source_blob = bucket.blob(source_blob_name)
        destination_blob = bucket.blob(destination_blob_name)
        
        # Copy to new location
        bucket.copy_blob(source_blob, bucket, destination_blob_name)
        
        # Delete from original location
        source_blob.delete()
        
        return True
        
    except Exception as e:
        print(f"✗ Error moving file {source_blob_name}: {e}")
        return False


def delete_file_from_gcs(bucket_name, storage_client, blob_name):
    """
    Delete a file from GCS
    
    Args:
        bucket_name: GCS bucket name
        storage_client: GCS client instance
        blob_name: Blob name to delete
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        
        return True
        
    except Exception as e:
        print(f"✗ Error deleting file {blob_name}: {e}")
        return False


def extract_table_name_from_filename(file_name):
    """
    Extract table name from CSV filename
    
    Args:
        file_name: CSV filename (e.g., 'user_events_20240114.csv')
        
    Returns:
        str: Table name (e.g., 'user_events')
    """
    # Remove .csv extension
    table, _ = os.path.splitext(file_name)
    
    # Split by underscore and join first two parts
    table_name_parts = table.split('_')
    table_name = "_".join(table_name_parts[:2])
    
    return table_name


def load_partition_to_postgresql(bucket_name, storage_client, file_name, pg_connection, table_name):
    """
    Load a single partition CSV file to PostgreSQL
    
    Args:
        bucket_name: GCS bucket name
        storage_client: GCS client instance
        file_name: CSV file name
        pg_connection: PostgreSQL connection
        table_name: Target table name
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Download file from GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f'processing_zone/{file_name}')
        
        buffer = BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        
        # Read CSV into DataFrame
        df = pd.read_csv(buffer)
        
        # Convert column names to lowercase
        df.columns = [c.lower() for c in df.columns]
        
        # Load to PostgreSQL
        if_exists = 'replace' if table_name == 'partitioned_table3' else 'append'
        
        df.to_sql(
            name=table_name,
            con=pg_connection,
            schema='public',
            chunksize=5000,
            index=False,
            if_exists=if_exists
        )
        
        return True
        
    except Exception as e:
        print(f"✗ Error loading {file_name}: {e}")
        return False


def load_all_partitions_to_postgresql():
    """
    Load all partitions from GCS processing_zone to PostgreSQL
    """
    try:
        # Get GCP clients
        gcp_clients = get_gcp_clients()
        gcs_client = gcp_clients.get_gcs_client()
        config = gcp_clients.get_config()
        
        # Get PostgreSQL connection
        pg_client = get_postgresql_connection()
        pg_connection = pg_client.get_postgresql_connection()
        
        # Get configuration
        bucket_name = config['gcs']['bucket_name']
        
        if not bucket_name:
            print("✗ Bucket Name not configured in config.yaml")
            return [], []
        
        # Get list of files in processing_zone
        gcs_files = get_gcs_files_in_processing_zone(bucket_name, gcs_client)
        
        if not gcs_files:
            return [], []
        
        # Process each file
        successful_files = []
        failed_files = []
        
        for file_name in gcs_files:
            table_name = extract_table_name_from_filename(file_name)
            
            # Try to load the partition
            success = load_partition_to_postgresql(
                bucket_name=bucket_name,
                storage_client=gcs_client,
                file_name=file_name,
                pg_connection=pg_connection,
                table_name=table_name
            )
            
            if success:
                # Remove from processing_zone (successful load)
                delete_success = delete_file_from_gcs(
                    bucket_name=bucket_name,
                    storage_client=gcs_client,
                    blob_name=f'processing_zone/{file_name}'
                )
                
                if delete_success:
                    successful_files.append(file_name)
                else:
                    failed_files.append(file_name)
            else:
                # Move to unprocess_zone (failed load)
                move_success = move_file_in_gcs(
                    bucket_name=bucket_name,
                    storage_client=gcs_client,
                    source_blob_name=f'processing_zone/{file_name}',
                    destination_blob_name=f'unprocess_zone/{file_name}'
                )
                
                if move_success:
                    failed_files.append(file_name)
        
        return successful_files, failed_files
        
    except Exception as e:
        print(f"✗ Error in load_all_partitions_to_postgresql: {e}")
        return [], []
    finally:
        # Close PostgreSQL connection
        if 'pg_connection' in locals():
            pg_connection.close()
        if 'pg_client' in locals():
            pg_client.close_postgresql_connection()


def main():
    """Main function to load partitions to PostgreSQL"""
    successful_files, failed_files = load_all_partitions_to_postgresql()
    return successful_files, failed_files


if __name__ == "__main__":
    main() 