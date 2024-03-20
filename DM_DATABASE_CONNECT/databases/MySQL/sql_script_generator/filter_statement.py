# this file searches for any filter condition for the given table name
# from the extraction table and creates a filter condition accordingly

from oops.object import mig_obj
from databases.MySQL.connection import get_data_as_data_frame
import pandas as pd
import re


FILTER_CONDITION_TABLE_NAME = mig_obj.environemnt_details['filter_conditions_table_name']


# # This function generated where clause for the given list of tables
# # conditions for the tables is fetched from database
# # function takes a list of tables and an alias dictinonary
# # if by default alias dicitionary is empty. If the user needs to replace the original table name with alias names, then alone 
# #       alias dict needs to be passed like this : {'table_name_1' : 'alias_name_1' , ..... 'table_name_n' : 'alias_name_n'}
# # df --> it holds the extraction rules data which will be read from the extraction rules tab for each mig obj
# def get_where_condition(table_names : list[str],alisas_dict : dict ={} , country : str = None ,  df : pd = None ):

#     # initially where condition is set empty. If there are no conditions for the given list of tables, then this function also returns blank value only
#     where_condition = ""

#     # this regexp is used to find if we need to add quotes or not
#     # Its used to find if the given values contains only numbers
#     # If the given value contains only numbers then quotes will not be added else quotes will be added
#     check_digits_pattern = re.compile('^\d*$')

#     # While extracting the filter coindition from DB, if we need to extract condition only for specific country then we add this below condition inside the below loop
#     # country_filter = '' if (country is None or len(country) == 0) else f' and COUNTRY = "{country}"'

#     # We loop through the given list of tables and then generate the where condition

#     if country is not None:
#         df= df[df["Country"]==country]

#     for table_name in table_names:
#         # sql_query = f'select sap_table,filter_column,filter_values,operation from {FILTER_CONDITION_TABLE_NAME} where SAP_TABLE = "{table_name}" {country_filter}'
#         # df = get_data_as_data_frame(sql_query= sql_query)

#         for records in df.to_records():

#             sap_table_name = records[1]
#             filter_column = records[2]
#             filter_values = records[3]
#             operation = records[4]

#             final_filter_value = ""

#             # we are checking for any alias name for the given table name .
#             # if there is any alias then we use that alias name only
#             if sap_table_name in alisas_dict:
#                 sap_table_name = alisas_dict[sap_table_name]

            
#             #if incase the the filter_values have "," then we split the filter_values into an array and then add quotes to varchar values if needed
#             try:
#                 filter_values_arr = filter_values.split(",")
#                 add_quotes = list(filter(check_digits_pattern.match,filter_values_arr)) == []
#                 if add_quotes:
#                     final_filter_value = ",".join(['"'+val.strip()+'"' for val in filter_values_arr])
#                 else:
#                     final_filter_value = ",".join(filter_values_arr)

#             # if there is no "," in filter_values then we simply add quotes to the filter_values if needed
#             except:
#                 add_quotes = check_digits_pattern.match(filter_values) is not None
#                 if add_quotes :
#                     final_filter_value = '"'+filter_values.strip()+'"'
#                 else:
#                     final_filter_value = filter_values

#             # if the operation is an "IN" then we add curly braces to the final_filer_value 
#             if operation.find('IN') != -1:
#                 final_filter_value = "(" + final_filter_value +")"

#             # finally where condition is generated
#             where_condition += ' where ' if len(where_condition) == 0 else ' and ' 
#             where_condition += f' {sap_table_name}.{filter_column} {operation} {final_filter_value} '
#     return where_condition



# This function generated where clause for the given list of tables
# conditions for the tables is fetched from database
# function takes a list of tables and an alias dictinonary
# if by default alias dicitionary is empty. If the user needs to replace the original table name with alias names, then alone 
#       alias dict needs to be passed like this : {'table_name_1' : 'alias_name_1' , ..... 'table_name_n' : 'alias_name_n'}
# df --> it holds the extraction rules data which will be read from the extraction rules tab for each mig obj
def get_where_condition(table_names : list[str],alisas_dict : dict ={} , country : str = None ,  df : pd.DataFrame = None ):

    # initially where condition is set empty. If there are no conditions for the given list of tables, then this function also returns blank value only
    where_condition = ""

    # this regexp is used to find if we need to add quotes or not
    # Its used to find if the given values contains only numbers
    # If the given value contains only numbers then quotes will not be added else quotes will be added
    # check_digits_pattern = re.compile('^\d*$')

    # While extracting the filter coindition from DB, if we need to extract condition only for specific country then we add this below condition inside the below loop
    # country_filter = '' if (country is None or len(country) == 0) else f' and COUNTRY = "{country}"'

    # We loop through the given list of tables and then generate the where condition

    if country is not None:
        df= df[(df["Country"]==country)]


        
    for table_name in table_names:
        # sql_query = f'select sap_table,filter_column,filter_values,operation from {FILTER_CONDITION_TABLE_NAME} where SAP_TABLE = "{table_name}" {country_filter}'
        # df = get_data_as_data_frame(sql_query= sql_query)

        temp_df = df[df["SAP_Table"]==table_name]
        
        no_of_cols_to_skip = 1
        for records in temp_df.to_records():

            sap_table_name = records[1+no_of_cols_to_skip]
            filter_column = records[2+no_of_cols_to_skip]
            filter_values = str(records[3+no_of_cols_to_skip])
            operation = records[4+no_of_cols_to_skip]

            # print(records)
            # print(operation)

            final_filter_value = ""

            # we are checking for any alias name for the given table name .
            # if there is any alias then we use that alias name only
            if sap_table_name in alisas_dict:
                sap_table_name = alisas_dict[sap_table_name]

            
            #if incase the the filter_values have "," then we split the filter_values into an array and then add quotes to varchar values if needed
            try:
                filter_values_arr = filter_values.split(",")
                # add_quotes = list(filter(check_digits_pattern.match,filter_values_arr)) == []
                # if add_quotes:
                final_filter_value = ",".join(['"'+val.strip()+'"' for val in filter_values_arr])
                # else:
                #     final_filter_value = ",".join(filter_values_arr)

            # if there is no "," in filter_values then we simply add quotes to the filter_values if needed
            except:
                # add_quotes = check_digits_pattern.match(filter_values) is not None
                # if add_quotes :
                final_filter_value = '"'+filter_values.strip()+'"'
                # else:
                #     final_filter_value = filter_values

            # if the operation is an "IN" then we add curly braces to the final_filer_value 
            if operation.find('IN') != -1:
                final_filter_value = "(" + final_filter_value +")"

            # finally where condition is generated
            where_condition += ' where ' if len(where_condition) == 0 else ' and ' 
            where_condition += f' {sap_table_name}.{filter_column} {operation} {final_filter_value} '
    return where_condition