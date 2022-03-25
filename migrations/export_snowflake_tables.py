from service.snowflake_db import connection
from common.logs import setup_logging, log_msg
import os

LOG = setup_logging(__name__)
FILE_NAME = "{snowflake_table}_{index}.csv"


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
            df.to_csv(file_path)

        log_msg(LOG, f'Done exporting {table} to {destination_folder}')
