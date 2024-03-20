import pandas as pd
from pyrfc import Connection
from oops.object import mig_obj
from databases.MySQL.connection import get_connection as get_mysql_conn
from databases.S4Hana.connection import get_connection as get_s4_Conn
from sqlalchemy import create_engine
import mysql.connector as sql


# this is the main dict used for converting the corresponding SAP datatype to mysql DATAtype
# if any SAP datatype is missed, by default it will be converted to varchar
sap_mysql_data_transform = {
    "CLNT" : 'VARCHAR',
    "CHAR" : "VARCHAR",
    "DATS" : "DATE",
    "DEC" : "DECIMAL", 
    "INT1" : "TINYINT UNSIGNED",
    "INT2" : "SMALLINT",
    "INT4" : "INT",
    "INT8" :  "BIGINT",
    "NUMC" : "CHAR",
    "QUAN" : "DECIMAL" ,
    "RAW" : "CHAR" ,
    "TIMS" : "TIME" ,
    "UNIT" : "VARCHAR",
    "CURR" : "DECIMAL",
    "ACCP" : "VARCHAR",
    "LANG" : "VARCHAR",
    "FLTP" : "DECIMAL",
    "CUKY" : "CHAR",
    "STRG" : "VARCHAR"
}


# this will extract data from 'RFC_READ_TABLE' function and returns the result without any blanks
# data -------------> pass the result of 'RFC_READ_TABLE' function as res["DATA"]
# key_column_name --> "key" value for identyfing the the records inside the data dict
# delimiter --------> splits the records based on this delimiter
# output --> [ [<table name1> , <column name1>, <data type1> , <char length1> , <no of decimals1>], .... ]
def get_data_as_list( 
        data : list[dict] , 
        key_column_name : str = 'WA' , 
        delimiter : str = mig_obj.environemnt_details["current_delimiter"]
    ) -> list[list[str]]:
    
    final_res = []
    
    # first we will loop thorugh all dict inside the data
    for record in data:
        
        # after accessing the data we will split it delimiter wise
        # then return remove the blank spaces 
        # then append the result to final_res
        final_res.append(list(map(lambda x : x.strip(),record[key_column_name].split(delimiter))))
        
    return final_res

# used to filter column names from meta data table dd03l
# which contains "." and "/" in their column names
# records = list[list[str]] --> output of function "get_data_as_list" is passed as input here
# column_index = index in which column names will be stored in the given "records"
def filter_fields_names( records : list[list[str]] , 
                         column_index : int = 1 , 
                         datatype_index :int = 2,
                         datalength_index : int = 3
                       ) -> tuple[list[str],list[str]]:
    
    # below filter condition is for excluding . and /
#     invalid_field_records = list(filter(lambda record : record[column_index].find(".")!=-1 or record[column_index].find("/")!=-1 , records))
#     valid_field_records = list(filter(lambda record : record[column_index].find(".")==-1 and record[column_index].find("/")==-1 , records))
    
    # below filter condition is for exculding . alone
#     invalid_field_records = list(filter(lambda record : record[column_index].find(".")!=-1 , records))
#     valid_field_records = list(filter(lambda record : record[column_index].find(".")==-1, records))
 
    invalid_field_records = []
    valid_field_records = []
    
    filter_function = lambda arr : [valid_field_records.append(x) if x[column_index].find(".")==-1 and  x[datatype_index].find("LRAW")!=0 and x[datalength_index].find("LRAW") <=350 else invalid_field_records.append(x) for x in arr]

    filter_function(records)
    
    # uncomment the below code for analysis
    print("Analysis : ")
    print(f"Total No of columns passed in : {len(records)}")
    print(f"Valid No of columns           : {len(valid_field_records)}")
    print(f"Invalid No of columns         : {len(invalid_field_records)}")
    print("\n=======================\n included fields : \n ")
    print(valid_field_records)
    print("\n=======================\n Excluded fields : \n ")
    print(invalid_field_records)
    print("\n=======================\n ")
    
    return valid_field_records , invalid_field_records


# used to extract the given table structure from sap 
# it returns the value in the format 
# table_info = {
#     "TABLE_NAME" : "<YOUR TABLE NAME>",
#     "COLUMNS" : ['FIELDNAME','DATATYPE','LENG','DECIMALS'],
#     "DATA" : "LIST[<CORRESPONDING COLUMN DATA>]",
# }
def get_table_structure_from_sap(table_name : str , s4conn : Connection) -> dict :
    
    # fields = ['TABNAME','FIELDNAME','MANDATORY','KEYFLAG','DATATYPE','LENG','DECIMALS']
    fields = mig_obj.environemnt_details["METADATA_FIELDS"].split(",")
    
    table_structure_raw_data = s4conn.call('RFC_READ_TABLE',QUERY_TABLE  = mig_obj.environemnt_details["METADATA_TABLE"] ,FIELDS = fields, OPTIONS = [{"TEXT":f"TABNAME = '{table_name}'"}],DELIMITER = mig_obj.environemnt_details["current_delimiter"] )
    table_structure = get_data_as_list(table_structure_raw_data['DATA'])
    valid_table_structure = filter_fields_names(table_structure)[0]
    table_info = dict(TABLE_NAME = table_name , COLUMNS = fields[1:] , DATA = [ rec[1:] for rec in valid_table_structure])
    return table_info



    
# used to create a create table statement for the given table structure
# table_info should have the following data
# table_info = {
#     "TABLE_NAME" : "<YOUR TABLE NAME>",
#     "COLUMNS" : ['FIELDNAME','DATATYPE','LENG','DECIMALS'],
#     "DATA" : "LIST[<CORRESPONDING COLUMN DATA>]",
# }
# def get_dtype_for_to_sql_function(table_info : dict) -> str:
    

#     dtype = {}
#     for field_name, sap_datatype, length, no_of_decimals in table_info["DATA"]:
#         try :
#             mysql_dt = sap_mysql_data_transform[sap_datatype]
#             if(mysql_dt.find("VARCHAR") > -1):
#                 dtype[field_name] = f"VARCHAR({length})"
#             elif(mysql_dt.find("CHAR") > -1):
#                 dtype[field_name] = f"CHAR({length})"
#             elif (mysql_dt.find("DECIMAL") > -1) :
#                 dtype[field_name] = f"DECIMAL({int(length)+10 if length == no_of_decimals  else length,no_of_decimals})"
#             else :
#                 dtype[field_name] =mysql_dt
#         except Exception as e :
            
#             print("There is no corresponding mysql datatype declared for : ",sap_datatype)
#             dtype[field_name] =f"VARCHAR({int(length)+int(no_of_decimals)})"
#             print(e)
    
    
#     return dtype


# used to create a create table statement for the given table structure
# table_info should have the following data
# table_info = {
#     "TABLE_NAME" : "<YOUR TABLE NAME>",
#     "COLUMNS" : ['FIELDNAME','DATATYPE','LENG','DECIMALS'],
#     "DATA" : "LIST[<CORRESPONDING COLUMN DATA>]",
# }
def get_mysql_create_table_statement(table_info : dict) -> str:
    

    
    create_statement = "CREATE TABLE "+table_info["TABLE_NAME"]+"("
    field_values = ""
    for field_name, sap_datatype, length, no_of_decimals in table_info["DATA"]:
        try :
            mysql_dt = sap_mysql_data_transform[sap_datatype]
            if(mysql_dt.find("CHAR") > -1):
                field_values += f"`{field_name}` {mysql_dt}({length}) ,"+"\n"
            elif (mysql_dt.find("DECIMAL") > -1) :
                field_values += f"`{field_name}` {mysql_dt}({int(length)+10 if length == no_of_decimals  else length},{no_of_decimals}) ,"+"\n"
            else :
                field_values += f"`{field_name}` {mysql_dt} ,"+"\n"
        except Exception as e :
            
            print("There is no corresponding mysql datatype declared for : ",sap_datatype)
            field_values += f"`{field_name}` VARCHAR({int(length)+int(no_of_decimals)}) ,"+"\n"
            print(e)
    
    create_statement += field_values[:-2] + "\n);"
    
    return create_statement

def get_mysql_insert_statement(table_name:str , table_data :list[list]) -> str:
    insert_statement = "insert into "+table_name+" values "
    
    for record in table_data :
        insert_statement += "("+ ",".join(['"'+item.replace('"','')+'"' for item in record]) + "),\n"
    
    return insert_statement[:-2]
     

# used to define range for reading data from SAP in order to avoid data buffer length error 
# table_info should have the following data
# table_info = {
#     "TABLE_NAME" : "<YOUR TABLE NAME>",
#     "COLUMNS" : ['FIELDNAME','DATATYPE','LENG','DECIMALS'],
#     "DATA" : "LIST[<CORRESPONDING COLUMN DATA>]",
# }
def get_ranges_to_read_table_data(table_info : dict):
    
    # here we will save the start_index, end_index values to read from the specified columns alone
    ranges :list[tuple[int,int]] = []
    data_length :int= 0
    start_index :int = 0
    i : int = 0
    while( i < len(table_info["DATA"])):
        data_length += int(table_info["DATA"][i][2]) + int(table_info["DATA"][i][3])
        # print(f"{ data_length = }")
        # print(f"{i = }")
        if data_length <= 350 :
            i+=1
        else :
            data_length = 0
            ranges.append((start_index,i))
            start_index = i
    if(start_index!=i):
        ranges.append((start_index,i))
    return ranges

# used to load table data from mysql to 
# table_info should have the following data
# table_info = {
#     "TABLE_NAME" : "<YOUR TABLE NAME>",
#     "COLUMNS" : ['FIELDNAME','DATATYPE','LENG','DECIMALS'],
#     "DATA" : "LIST[<CORRESPONDING COLUMN DATA>]",
# }
# max_no_of_rows_to_read ---> it controls now many records can be loaded at a time
#                             Values should be in range 1000 - 20000
#                             Below 1000 and above 20K perfomance decreases
def load_table_in_mysql(table_info : dict , 
                        s4conn : Connection , 
                        mysql_connector : sql.connect,
                        mysqlengine : create_engine,
                        max_no_of_rows_to_read : int = int(mig_obj.environemnt_details["no_of_rows_per_batch"]),
                        delimiter : str = mig_obj.environemnt_details["current_delimiter"],
                        max_records_to_load = mig_obj.environemnt_details["max_records_to_load"],
                        
                       ) -> None:
    
    print(f"{'Going to get ranges':_^85}")
    columns = [row[0] for row in table_info["DATA"]]
    column_ranges = get_ranges_to_read_table_data(table_info=table_info)
    
    result = s4conn.call("EM_GET_NUMBER_OF_ENTRIES", IT_TABLES=[{"TABNAME": table_info["TABLE_NAME"]}])
    no_of_records = result["IT_TABLES"][0]["TABROWS"]
    no_of_row_to_read = min(max_no_of_rows_to_read,no_of_records)
    # max_records_to_load = min(no_of_records,max_records_to_load) if max_records_to_load is None else min(no_of_records,max_records_to_load)
    try :
        max_records_to_load = min(no_of_records,int(max_records_to_load))
    except :
        max_records_to_load = no_of_records
    no_of_records_loaded = 0
    # mysql_cursor = mysql_connector.cursor()
    
    column_datatype_dict = dict([[row[0],row[1]] for row in table_info["DATA"]])
    
    # dtype = get_dtype_for_to_sql_function(table_info)
    
    # final_insert_fields = ""
    
    # this will store the column names whoses data type = DECIMAL
    # we use this to convert sap values decimal values like "23.09-" to "-23.09"
    # while trying to insert "23.09-" as such in mysql will throw an error. To avoid that we use this list of column names to manipulate data in df
    decimal_columns = []
    
    for key in column_datatype_dict.keys():
        if(sap_mysql_data_transform[column_datatype_dict[key]] == "DECIMAL" ):
            decimal_columns.append(key)
            
    
    print("\n"+f"{'Total Records at source ' :-<45} : {no_of_records}"+"\n")
    while( no_of_records_loaded < max_records_to_load):
        # creating an empty dataframe
        final_df = pd.DataFrame()

        for start_index,end_index in column_ranges :
            # print("debug : ")
            # print(columns[start_index:end_index])
            data = s4conn.call('RFC_READ_TABLE' , QUERY_TABLE = table_info["TABLE_NAME"] , FIELDS = columns[start_index:end_index] , DELIMITER = delimiter ,ROWSKIPS = no_of_records_loaded , ROWCOUNT = no_of_row_to_read )
            
            trimmed_data = get_data_as_list(data["DATA"])
            
            df = pd.DataFrame(data = trimmed_data, columns = [column_name["FIELDNAME"] for column_name in data["FIELDS"]])

            final_df = pd.concat([final_df,df],axis = 1)

        
        no_of_records_loaded+=len(df)
        
        # logic to convert "23.09-" to "-23.09"
        for decimal_column in decimal_columns:
            final_df[decimal_column] = final_df[decimal_column].apply(lambda x : '-'+x[:-1] if x[-1] =='-' else x)
        
        
        try : 
            # mysql_cursor.execute(insert_statement)
            final_df.to_sql(name=table_info['TABLE_NAME'].lower(),con = mysqlengine , index = False ,if_exists = 'append')
            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print(f"Inserted {no_of_records_loaded - len(df)} - {no_of_records_loaded} records ")
            print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        except Exception as e:
            print("\n==============================\nCouldn't insert records . The error is : \n",e)

            print("\n\n===============================")
    
    if (no_of_records <= 0) :
        print("\n\n````````````````````````````````````````````````````````````````")
        print(" There is no data for the table : ",table_info["TABLE_NAME"] )
        print("````````````````````````````````````````````````````````````````")
    else :

        print("\n\n````````````````````````````````````````````````````````````````")
        print(" DATA LOADING OVER FOR TABLE : ",table_info["TABLE_NAME"] )
        print("````````````````````````````````````````````````````````````````")
    
    # mysql_cursor.close()
        

# used for loading the sap data into mysql staging layer
def load_table_to_staging_layer(table_name :str ) -> None :
    
    mysqlconn = get_mysql_conn()[0]
    mysqlengine = create_engine(f"mysql+mysqlconnector://{mig_obj.environemnt_details['user']}:{mig_obj.environemnt_details['password']}@{mig_obj.environemnt_details['host']}/{mig_obj.environemnt_details['database']}")
    s4conn = get_s4_Conn()
    
    table_name = table_name.upper()
    mysql_cursor = mysqlconn.cursor()
    # we get the basic metadata of the table from sap system
    table_info = get_table_structure_from_sap(table_name = table_name , s4conn =  s4conn)
    
    if(len(table_info["DATA"]) > 0 ) :
        step_no = 1
        # first we check if the table structure for the give table name is present in backend or not.
        # if present we will truncate the table then load the data
        # if not present we will create the structure then load the data
        try :
            sql_query = f"truncate table {table_name}"
            
            print("\n"+f"{'WORK LOG':*^85}"+"\n")
            print(f"STEP {step_no :-<25}> Truncating table : {table_name}"+"\n")
            mysql_cursor.execute(sql_query)
        except Exception as e:
            step_no+=1
            print(f"STEP {step_no :-<25}> There is no table in the staging layer. Creating the structure for table : {table_name} "+"\n")
            create_table_statement = get_mysql_create_table_statement(table_info = table_info)
            try : 
                mysql_cursor.execute(create_table_statement)
                print(f"{'' :<31}  Strucutre successfully created for table : {table_name}"+"\n")
            except Exception as e :
                print("\n------------\nSome thing unexpected happened . The error is \n ",e,"\n-------------------\n")
                mysql_cursor.close()
                mysqlconn.close()
                s4conn.close()
                exit()
        mysql_cursor.close()
        step_no+=1
        print(f"STEP {step_no :-<25}> Going to load data into STGING layer for table : {table_name}"+"\n")
        load_table_in_mysql(table_info =table_info , s4conn = s4conn , mysql_connector = mysqlconn , mysqlengine = mysqlengine)
        s4conn.close()
        mysqlconn.close()
    else :
        print("There is no table ",table_name," in SAP. Please check the table name")
        

