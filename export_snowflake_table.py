from migrations.export_snowflake_tables import export_table_to_csv
import argparse


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('snowflake_database')
    p.add_argument('snowflake_schema')
    p.add_argument('snowflake_table')
    p.add_argument('destination_folder')

    kwargs = vars(p.parse_args())
    export_table_to_csv(**kwargs)