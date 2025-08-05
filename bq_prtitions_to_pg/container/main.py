"""
Main script to orchestrate BigQuery to PostgreSQL partition transfer
"""
import sys
import os
from datetime import datetime

# Add current directory to path to import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scripts.export_bq_partitions_to_gcs import main as export_partitions
from scripts.load_partitions_to_pg import main as load_partitions


def main():
    """
    Main orchestration function for BigQuery to PostgreSQL partition transfer
    """
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Step 1: Export BigQuery partitions to GCS
        exported_tables = export_partitions()
        
        if not exported_tables:
            print("No partitions to export")
            return
        
        # Step 2: Load partitions from GCS to PostgreSQL
        successful_files, failed_files = load_partitions()
        
        # Summary
        print(f"Completed: {len(successful_files)} loaded, {len(failed_files)} failed")
        
    except KeyboardInterrupt:
        print("Interrupted")
        return
    except Exception as e:
        print(f"Error: {e}")
        return


if __name__ == "__main__":
    main() 