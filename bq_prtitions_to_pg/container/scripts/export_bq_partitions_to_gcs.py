"""
Script to check for yesterday's partitions in BigQuery tables and export to GCS
"""
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden, GoogleAPIError
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# Add parent directory to path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gcp_clients.clients import get_gcp_clients


def list_partitioned_tables(bq_client, dataset_id):
    """
    List all partitioned tables in a BigQuery dataset
    
    Args:
        bq_client: BigQuery client instance
        dataset_id: Dataset ID to check
        
    Returns:
        list: List of partitioned table IDs
    """
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        tables = bq_client.list_tables(dataset_ref)
        partitioned_tables = []

        for table in tables:
            try:
                table_ref = bq_client.get_table(table.reference)
                if table_ref.time_partitioning or table_ref.range_partitioning:
                    partitioned_tables.append(table.table_id)
            except NotFound:
                print(f"✗ Table {table.table_id} not found.")
            except Forbidden:
                print(f"✗ Access to table {table.table_id} is forbidden.")

        return partitioned_tables
    except NotFound:
        print(f"✗ Dataset {dataset_id} not found.")
        return []
    except Forbidden:
        print(f"✗ Access to dataset {dataset_id} is forbidden.")
        return []
    except GoogleAPIError as e:
        print(f"✗ An error occurred: {e}")
        return []


def check_for_yesterdays_partition(bq_client, table, full_dataset_id, yesterday):
    """
    Check if yesterday's partition exists for a specific table
    
    Args:
        bq_client: BigQuery client instance
        table: Table ID to check
        full_dataset_id: Full dataset ID (project.dataset)
        yesterday: Yesterday's date in YYYY-MM-DD format
        
    Returns:
        str or None: Partition ID if found, None otherwise
    """
    new_partition = yesterday.replace('-', '')

    query = f"""
    SELECT MAX(partition_id) as max_partition_id
    FROM `{full_dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = '{table}'
    """
    
    try:
        query_job = bq_client.query(query)
        result = query_job.result()
        partition_id = next(result, None).max_partition_id if result.total_rows > 0 else None
        
        if partition_id == new_partition:
            return partition_id
        else:
            return None
            
    except bigquery.exceptions.BigQueryError as e:
        print(f"✗ BigQuery error processing table {table}: {e}")
    except Exception as e:
        print(f"✗ Unexpected error processing table {table}: {e}")

    return None


def get_yesterday_date():
    """Get yesterday's date in YYYY-MM-DD format"""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')


def get_partitioning_field(client, table, dataset_id):
    """
    Get the partitioning field for a BigQuery table
    
    Args:
        client: BigQuery client instance
        table: Table ID
        dataset_id: Dataset ID
        
    Returns:
        str or None: Partitioning field name if found, None otherwise
    """
    table_id = f'{dataset_id}.{table}'
    try:
        bq_table = client.get_table(table_id)
        if bq_table.time_partitioning:
            return bq_table.time_partitioning.field
        elif bq_table.range_partitioning:
            return bq_table.range_partitioning.field
        else:
            return None
    except bigquery.NotFound as e:
        print(f"✗ Table {table_id} not found: {e}")
        return None
    except bigquery.Forbidden as e:
        print(f"✗ Access to table {table_id} is forbidden: {e}")
        return None
    except bigquery.GoogleCloudError as e:
        print(f"✗ Google Cloud error accessing table {table_id}: {e}")
        return None
    except Exception as e:
        print(f"✗ Unexpected error getting partitioning field for table {table_id}: {e}")
        return None


def export_partition_to_csv(bucket_name, storage_client, bq_client, dataset_id, table, 
                           partition_field, partition_id, yesterday, destination_uri, location="us-west1"):
    """
    Export a BigQuery partition to GCS as CSV
    
    Args:
        bucket_name: GCS bucket name
        storage_client: GCS client instance
        bq_client: BigQuery client instance
        dataset_id: Dataset ID
        table: Table ID
        partition_field: Partitioning field name
        partition_id: Partition ID (e.g., '20240114')
        yesterday: Yesterday's date in YYYY-MM-DD format
        destination_uri: Base destination URI
        location: GCS location (default: us-west1)
    """
    destination_uri = f'{destination_uri}/{table}_{partition_id}.csv'
    blob_name = f'processing_zone/{table}_{partition_id}.csv'

    query = f"""
    SELECT *
    FROM `{dataset_id}.{table}`
    WHERE `{partition_field}` = '{yesterday}'
    """

    try:
        # Execute query and get results
        query_job = bq_client.query(query)
        results = query_job.result()
        
        # Convert to DataFrame
        df = results.to_dataframe()
        
        if df.empty:
            return False
        
        # Upload to GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        # Convert DataFrame to CSV and upload
        csv_data = df.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type='text/csv')
        
        return True
        
    except GoogleAPIError as e:
        print(f"✗ Error exporting table {table} due to a Google Cloud error: {e}")
        return False
    except Exception as e:
        print(f"✗ General error exporting table {table}: {e}")
        return False


def export_all_new_partitions_to_gcs():
    """
    Export all new partitions (yesterday's partitions) to GCS as CSV files
    """
    try:
        # Get GCP clients
        gcp_clients = get_gcp_clients()
        bq_client = gcp_clients.get_bigquery_client()
        gcs_client = gcp_clients.get_gcs_client()
        config = gcp_clients.get_config()
        
        # Get configuration
        project_id = config['bigquery']['project_id']
        dataset_id = config['bigquery']['dataset_id']
        bucket_name = config['gcs']['bucket_name']
        
        if not project_id or not dataset_id or not bucket_name:
            print("✗ Project ID, Dataset ID, or Bucket Name not configured in config.yaml")
            return []
        
        full_dataset_id = f"{project_id}.{dataset_id}"
        yesterday = get_yesterday_date()
        partition_id = yesterday.replace('-', '')
        
        # List partitioned tables
        partitioned_tables = list_partitioned_tables(bq_client, dataset_id)
        
        if not partitioned_tables:
            return []
        
        # Check which tables have yesterday's partition and export them
        exported_tables = []
        failed_tables = []
        
        for table in partitioned_tables:
            # Check if yesterday's partition exists
            partition_exists = check_for_yesterdays_partition(bq_client, table, full_dataset_id, yesterday)
            
            if partition_exists:
                # Get partitioning field
                partition_field = get_partitioning_field(bq_client, table, dataset_id)
                
                if partition_field:
                    # Export to GCS
                    destination_uri = f"gs://{bucket_name}/processing_zone"
                    success = export_partition_to_csv(
                        bucket_name=bucket_name,
                        storage_client=gcs_client,
                        bq_client=bq_client,
                        dataset_id=full_dataset_id,
                        table=table,
                        partition_field=partition_field,
                        partition_id=partition_id,
                        yesterday=yesterday,
                        destination_uri=destination_uri
                    )
                    
                    if success:
                        exported_tables.append(table)
                    else:
                        failed_tables.append(table)
                else:
                    failed_tables.append(table)
        
        return exported_tables
        
    except Exception as e:
        print(f"✗ Error in export_all_new_partitions_to_gcs: {e}")
        return []


def main():
    """Main function to check yesterday's partitions and optionally export to GCS"""
    try:
        # Get GCP clients
        gcp_clients = get_gcp_clients()
        bq_client = gcp_clients.get_bigquery_client()
        config = gcp_clients.get_config()
        
        # Get configuration
        project_id = config['bigquery']['project_id']
        dataset_id = config['bigquery']['dataset_id']
        
        if not project_id or not dataset_id:
            print("✗ Project ID or Dataset ID not configured in config.yaml")
            return []
        
        full_dataset_id = f"{project_id}.{dataset_id}"
        yesterday = get_yesterday_date()
        
        # List partitioned tables
        partitioned_tables = list_partitioned_tables(bq_client, dataset_id)
        
        if not partitioned_tables:
            return []
        
        # Check each table for yesterday's partition
        tables_with_yesterday_partition = []
        
        for table in partitioned_tables:
            partition_id = check_for_yesterdays_partition(bq_client, table, full_dataset_id, yesterday)
            if partition_id:
                tables_with_yesterday_partition.append(table)
        
        # Export to GCS if tables have partitions
        if tables_with_yesterday_partition:
            exported_tables = export_all_new_partitions_to_gcs()
            return exported_tables
        
        return tables_with_yesterday_partition
        
    except Exception as e:
        print(f"✗ Error in main function: {e}")
        return []


if __name__ == "__main__":
    main() 