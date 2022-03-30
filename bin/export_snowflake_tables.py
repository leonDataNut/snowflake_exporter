from migrations.export_snowflake_tables import export_schema_tables_to_csv
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process for exporting tables from snowflake to a folder. By default the folder will be Documents')
    parser.add_argument('folder',type=str, nargs='?', const='Documents', default='Documents')
    args = parser.parse_args()
    export_schema_tables_to_csv(folder=args.folder)