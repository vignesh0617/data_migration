from databases.MySQL.sql_script_generator.filter_statement import get_where_condition
from databases.MySQL.connection import get_connection

def extract_source(table_name:str,country:str):
    select_statement = f"select * from {table_name}"
    where_statement =  get_where_condition(table_names=[table_name] , country= country)
    final_statemet = select_statement + where_statement
    return final_statemet

def extract_data_to_extraction_db(staging_table_name : str , sql_query : str, extraction_database_name : str  ):
    try:
        conn,cursor = get_connection(database_name=extraction_database_name)
        cursor.execute(f"create table {extraction_database_name}.{staging_table_name} as {sql_query}")
        cursor.close()
        conn.close()
    except Exception as e:
        print("Can not Load data from staging layer to Extraction layer . The error is :\n")
        print(e)

    