import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import glob
import os
from psycopg2.extras import execute_values

# --- DB connection stuff ---
DB_NAME = "store"
DB_USER = "postgres"
DB_PASS = "admin"

def initialize_database(schemas):
    """nuke the old DB if it exists and create a fresh one with schemas present"""

    con = psycopg2.connect(f"user={DB_USER} password={DB_PASS}")
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    try:  
        # Close all current Database connections so that we can drop it
        cur.execute(
                    f"""SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = '{DB_NAME}' 
                        AND pid <> pg_backend_pid();"""
                   )

        cur.execute(sql.SQL(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        cur.execute(sql.SQL(f"CREATE DATABASE {DB_NAME}"))
        print(f"Database '{DB_NAME}' created successfully\n")
    
    finally:
        # close initial connection
        cur.close()
        con.close()
    
    # connect to our new DB
    conn = psycopg2.connect(f"dbname={DB_NAME} user={DB_USER} password={DB_PASS}")
    cur = conn.cursor()
    
    # create all schemas from our input file
    for schema in schemas:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        conn.commit()
        print(f"Schema '{schema}' created successfully")
    print("")
    return conn, cur  # return both so we can use them later

def csv_to_list_of_dicts(csv_filepath):
    """Takes CSV file path and returns a list of dictionaries to represent the data in the file."""

    try:
        # pandas to the rescue - handles all the CSV parsing headaches
        df = pd.read_csv(csv_filepath,                 
                         quotechar='"',         # deal with quoted text
                         skipinitialspace=True, # ignore spaces after commas
                         escapechar='\\')       # handle escape chars
                         
        
        return df.to_dict('records')
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_filepath}")
        return []
    except pd.errors.EmptyDataError:
         print(f"Error: CSV file is empty: {csv_filepath}")
         return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

def insert_list_of_dicts(conn, table_name, data, schema=None):
    """Takes a table name and data represented as a list of dictionaries and loads the data 
       into a postgres database with a table of the same name"""

    if not data:
        return
    
    # Get the column names from the first dictionary
    columns = list(data[0].keys())
    
    # Create the SQL query with schema support
    if schema:
        fully_qualified_table = f'{schema}.{table_name}'
    else:
        fully_qualified_table = f'{table_name}'
    
    # Properly format column names 
    formatted_columns = ', '.join([f'{col}' for col in columns])
    
    # Create the query
    query = f"INSERT INTO {fully_qualified_table} ({formatted_columns}) VALUES %s"
    
    # Convert list of dicts to list of tuples in the correct order
    values = [[record.get(col) for col in columns] for record in data]
    
    # Execute the query
    with conn.cursor() as cursor:
        execute_values(cursor, query, values)
    
    conn.commit()
    print(f"Data from CSV file has been loaded into {schema}.{table_name}")

def validate_insertion(csv_data, conn, table_name, schema=None):
    """Validates that all records from the CSV were inserted into the database table"""
    
    # Get count of records in the original CSV data
    csv_record_count = len(csv_data)
    
    # Build the query to count records in the database table
    if schema:
        fully_qualified_table = f'{schema}.{table_name}'
    else:
        fully_qualified_table = f'{table_name}'
    
    count_query = f"SELECT COUNT(*) FROM {fully_qualified_table}"
    
    # Execute the query
    with conn.cursor() as cursor:
        cursor.execute(count_query)
        db_record_count = cursor.fetchone()[0]
    
    # Compare counts
    if csv_record_count == db_record_count:
        print(f"✓ Validation successful: All {csv_record_count} records inserted into {fully_qualified_table}")
        return True
    else:
        print(f"⚠ Validation failed: {csv_record_count} records in CSV but {db_record_count} in database table {fully_qualified_table}")
        return False
def main():
    """Main function. Create the tables and load the data."""

    # grab all CSVs from input dir
    csv_files = glob.glob("./input/*")
    csv_files = [file for file in csv_files if '\INFORMATION_SCHEMA.csv' not in file]

    if os.path.isfile('./input/INFORMATION_SCHEMA.csv'): #If the INFORMATION_SCHEMA.csv exists
        
        print('Schema file is present. Continuing with database initialization.\n')

        # load our schema info
        schema = csv_to_list_of_dicts('./input/INFORMATION_SCHEMA.csv')

        # extract unique schemas
        unique_table_schemas = {record['TABLE_SCHEMA'] for record in schema}

        # get unique combos of schema + table
        unique_tables = {(record['TABLE_SCHEMA'], record['TABLE_NAME']) for record in schema}
        
        # setup postgres server
        conn, cur = initialize_database(unique_table_schemas)

        try:
            # loop through and create each table-schema pair
            for table in unique_tables:
                table_schema, table_name = table

                # filter all columns for ones just in this table and sort by position
                table_columns = [col for col in schema if (col['TABLE_SCHEMA'], col['TABLE_NAME']) == table]
                table_columns = sorted(table_columns, key=lambda x: x['ORDINAL_POSITION'])

                # build column definitions
                column_strings = [f"{col['COLUMN_NAME']} {col['DATA_TYPE']}" for col in table_columns]

                # join with commas for SQL syntax
                comma_separated_columns = ', '.join(column_strings)

                # our final create table statement
                sql_cmd = f"""CREATE TABLE {table_schema}.{table_name}({comma_separated_columns})"""

                print(sql_cmd)
                
                cur.execute(sql_cmd)
                conn.commit()

                print(f"""Table {table_schema}.{table_name} has been created.\n""")

                
                #Load CSV data for the table-schema just created into a dictionary
                table_csv = [ file for file in csv_files if table_name in file.split("\\")[-1] ][0]
                data_to_load = csv_to_list_of_dicts(table_csv)
                
                # Find the columns to load for the schema-table pair
                cols_to_load = [col["COLUMN_NAME"] for col in table_columns]

                # one-liner to filter dictionaries for only columns in this schema-table pair
                filtered_data = [{col: record[col] for col in cols_to_load if col in record.keys()} for record in data_to_load]

                # load data into newely created schema-table pair into the database itself
                insert_list_of_dicts(conn, table_name, filtered_data, table_schema)

                validate_insertion(filtered_data, conn, table_name, table_schema)
                
                print('--------------------------------------------------------------')            
       
            print()            

        finally:
            # clean up DB connections
            cur.close()
            conn.close()
            print("Database connection closed")
    else:
        print('INFORMATION_SCHEMA.CSV is not present. Please verify it exists in the input folder.')

if __name__ == "__main__":
    main()
