from service.snowflake_db import connection
from common.logs import setup_logging, log_msg
import os
from migrations import EXPORT_TABLE_SCHEMAS, LIST_TABLES_SQL

LOG = setup_logging(__name__)
FILE_NAME = "{snowflake_table}_{index}.csv"
DEFAULT_FOLDER = os.environ.get('EXPORT_FOLDER','Documents')


def make_directory(folder):
    try:
        log_msg(LOG, f'Creating folder path {folder}')
        os.makedirs(folder)
    except:
        pass


def export_table_to_csv(snowflake_database, snowflake_schema, snowflake_table, destination_folder, verbose=True):
    table = "{}.{}.{}".format(snowflake_database,snowflake_schema,snowflake_table)
    query = f"SELECT * FROM {table}"
    
    log_msg(LOG, f'Exporting table {table} to folder {destination_folder}...')
    make_directory(destination_folder)

    with connection() as conn:
        results = conn.yield_query_results(query, verbose)

        for i,df in enumerate(results, 1):
            file_name = FILE_NAME.format(snowflake_table=snowflake_table, index=i)
            file_path = f"{destination_folder}/{file_name}"
            try:
                df.to_csv(file_path,index=False)
            except Exception as e:
                log_msg(LOG, f'Batch failed {e}')
        log_msg(LOG, f'Done exporting {table} to {destination_folder}')


def list_tables_in_schema(snowflake_database, snowflake_schema):
    query = LIST_TABLES_SQL.format(database=snowflake_database, schema=snowflake_schema)
    tables = []

    with connection() as conn:
        tables.extend(conn.execute_sql_return_column_values(query, 'TABLE_NAME', verbose=False))
    
    return tables


def export_tables_to_csv(snowflake_database, snowflake_schema, tables:list, folder=DEFAULT_FOLDER, verbose=True):
    for snowflake_table in tables:
        destination_folder = f"{folder}/{snowflake_database}-{snowflake_schema}-{snowflake_table}"
        export_table_to_csv(snowflake_database, snowflake_schema, snowflake_table, destination_folder, verbose)

 
def export_schema_tables_to_csv(schemas:list = EXPORT_TABLE_SCHEMAS, folder=DEFAULT_FOLDER, verbose=True):
    for schema in schemas:
        log_msg(LOG, f"Proccessing Tables in schema {schema}..." )
        snowflake_database = schema.split('.')[0]
        snowflake_schema = schema.split('.')[1]
        tables = list_tables_in_schema(snowflake_database, snowflake_schema)
        tables_str = "\n".join([f"{snowflake_database}.{snowflake_schema}.{x}" for x in tables])
        log_msg(LOG, f"Exporting Tables:\n{tables_str}" )
        export_tables_to_csv(snowflake_database, snowflake_schema, tables, folder, verbose=verbose)