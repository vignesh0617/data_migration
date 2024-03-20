import mysql.connector as mydb
import pandas as pd
from oops.object import mig_obj

def get_connection(database_name : str) :
    connector = mydb.connect(
        user = mig_obj.environemnt_details['user'],
        password =mig_obj.environemnt_details['password'],
        host =mig_obj.environemnt_details['host'],
        database =database_name,
        autocommit = True )
    
    return connector,connector.cursor()

def get_data_as_list(sql_query : str , database_name : str) -> list[tuple]:
    try :
        con , cur = get_connection(database_name)
        cur.execute(sql_query)
        res = cur.fetchall()
        cur.close()
        con.close()
        return res
    except Exception as e:
        print(f'The sql query = {sql_query}')
        print('-----------------The exception is ---------------------\n',e)

# returns table data from the selected database as a dataframe obj
def get_data_as_data_frame(sql_query,database_name : str) -> pd.DataFrame:
    try :
        new_connector, new_cursor = get_connection(database_name)
        new_cursor.execute(sql_query)
        fields = new_cursor.description
        data = new_cursor.fetchall()
        column_labels = [row[0] for row in fields]
        new_cursor.close()
        new_connector.close()
        return pd.DataFrame(data = data, columns= column_labels)
    except Exception as e :
        print(f'The sql query = {sql_query}')
        print('-----------------The exception is ---------------------\n',e)