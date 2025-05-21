from pathlib import Path
import sqlite3
import json
import sys
from typing import Dict, Any, List
from query_engine_client import QueryEngineClient
from container_params import ContainerParams, ContainerParamError

def fetch_all_rows_as_dicts(db_path: Path) -> List[Dict[str, Any]]:
    """Query the SQLite database and return all rows from the 'results' table
    as a list of dictionaries.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        List of dictionaries, where each dictionary represents a row
        and keys are column names. An empty list is returned if the
        table is empty or an error occurs before populating results.

    Raises:
        sqlite3.Error: If there's an SQLite specific error during database interaction.
        Exception: For other errors encountered during database processing.
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query all data from the results table
        cursor.execute('SELECT * FROM results')

        rows = cursor.fetchall()
        
        # Get column names from cursor.description
        # Column names will be strings. cursor.description might be None if the query doesn't return rows (e.g. failed query)
        # or if the results table is empty and cursor.execute wasn't called or table doesn't exist.
        # However, an empty 'results' table would yield rows=[] and description would still list columns.
        # If 'results' table doesn't exist, sqlite3.Error will be raised.
        if rows and cursor.description:
            column_names = [str(description[0]) for description in cursor.description]
        else:
            # Handle cases like empty table or no columns effectively leading to no data to structure.
            # If rows is empty, this part is not strictly needed as the loop below won't run.
            # If description is None for some reason (shouldn't happen for SELECT * on existing table),
            # this ensures column_names is empty or handled, preventing errors.
            column_names = []


        # Create a list of dictionaries
        results_list = []
        for row in rows:
            results_list.append(dict(zip(column_names, row)))

        conn.close()
        return results_list
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        raise
    except Exception as e:
        print(f"Error querying database: {e}")
        raise

def save_stats_to_json(data: Any, output_path: Path) -> None: # data type hint changed to Any, as it can be List or Dict
    """Save data to a JSON file.
    
    Args:
        data: Data to save (JSON serializable)
        output_path: Path where the JSON file will be saved
        
    Raises:
        Exception: If there's an error creating the output directory or saving the file
    """
    try:
        # Ensure the output directory exists
        output_dir = output_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to JSON
        with open(output_path, "w") as f:
            json.dump(data, f, indent=4)
            
        print(f"Stats saved to {output_path}")
    except Exception as e:
        print(f"Error saving JSON: {e}")
        raise

def execute_query(params: ContainerParams) -> bool:
    """Execute the query using the query engine client.
    
    Args:
        params: Container parameters with query details
        
    Returns:
        True if query execution was successful, False otherwise
    """
    if not params.validate_production_mode():
        return False
        
    # Initialize query engine client
    query_engine_client = QueryEngineClient(
        params.query, 
        params.query_signature, 
        str(params.db_path)
    )
    
    # Execute query
    print(f"Executing query: {params.query}")
    query_result = query_engine_client.execute_query(
        params.compute_job_id, 
        params.data_refiner_id,
        params.query_params
    )
    
    if not query_result.success:
        print(f"Error executing query: {query_result.error}")
        return False
        
    print(f"Query executed successfully, processing results from {params.db_path}")
    return True

def process_results(params: ContainerParams) -> None:
    """Process query results and generate stats file.
    
    Args:
        params: Container parameters
    """
    # Call the data fetching function
    all_rows_data = fetch_all_rows_as_dicts(params.db_path)
    
    if all_rows_data:
        print(f"Found {len(all_rows_data)} rows in the database")
        save_stats_to_json(all_rows_data, params.stats_path)
    else:
        print("No data found in the results table")
        # Create an empty list in the JSON file to indicate processing completed but no data
        save_stats_to_json([], params.stats_path)

def main() -> None:
    """Main entry point for the worker."""
    try:
        # Load parameters from environment variables
        try:
            params = ContainerParams.from_env()
        except ContainerParamError as e:
            print(f"Error in container parameters: {e}")
            sys.exit(1)
        
        # Handle development vs production mode
        if params.dev_mode:
            print("Running in DEVELOPMENT MODE - using local database file")
            print(f"Processing query results from {params.db_path}")
        else:
            # In production mode, execute the query first
            if not execute_query(params):
                sys.exit(2)
        
        # Process results (whether from dev mode or query execution)
        process_results(params)
        
    except Exception as e:
        print(f"Error in worker execution: {e}")
        sys.exit(3)

if __name__ == "__main__":
    main()