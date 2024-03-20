import pandas as pd
from oops.data_mig_obj import Joins

joins = Joins()

# a select query is generated based on excel file with the below columns
# 1st COL : Source table name
# 2nd col : Field name
# based on these 2 inputs we are generating an select statement
def get_select_query(data_frame:pd.DataFrame,source_table_number : int , source_field_number : int , database_name : str):
    rows = data_frame.shape[0]
    unique_tables = []
    uniqe_selected_items = set()
    aliases = {}
    ascii_number = 65 # 65 = "A" in ascii chars
    # cols = data_frame.shape[1]
    selected_items = 'select '
    for row in range(rows):
        source_table = str(data_frame.iloc[row,source_table_number-1]).strip()
        source_fields = str(data_frame.iloc[row,source_field_number-1])
        source_fields_list = source_fields.split('\n')

        if source_table not in unique_tables : unique_tables.append(source_table)

        if aliases.get(source_table) is None:
            aliases[source_table] = chr(ascii_number)
            ascii_number+=1

        for source_field in source_fields_list:
            field = source_field.strip()
            current_selected_field = aliases[source_table]+'.'+field
            if field!= "" and current_selected_field not in uniqe_selected_items:
                selected_items = selected_items+current_selected_field+',\n'
                uniqe_selected_items.add(current_selected_field)
        # uncomment the below two lines for checking how the fields are getting selected from the excel data
        #         print(source_table+'.'+field)
        # print("=========================")
    selected_items = selected_items[:-2]+'\nfrom '

    # LOGIC FOR CREATING JOIN CONDITION STATEMENT STARTS HERE : 
    final_index = len(unique_tables)

    #IF PART WILL EXECUTE ONLY WHEN THERE ARE MORE THAN 1 TABLE IN SELECT STATEMENT
    if final_index > 1 :
        join_conditions = ''
        check_joins = []
        messages = []
        for i in range(final_index):
            table_one = unique_tables[i]
            if i == final_index-1 and table_one not in check_joins:
                messages.append(f"No Join condition is defined for table : {table_one}")
                break
            for j in range(i+1,final_index):
                table_two = unique_tables[j]

                if table_one in check_joins and table_two in check_joins:
                    continue
                
                join_condition = joins.get_join_condition_from_excel(table_one=table_one,table_two=table_two)
                if join_condition.find("Error") ==-1:
                    join_condition = join_condition.replace(table_one,aliases[table_one])
                    join_condition = join_condition.replace(table_two,aliases[table_two])
                    join_conditions += f'\n {database_name+"."+table_one+" "+aliases[table_one]+" inner join "+database_name+"."+table_two+" "+aliases[table_two]+" on "+join_condition  if (table_one not in check_joins) else " inner join "+database_name+"."+table_two+" "+aliases[table_two]+" on "+join_condition }'
                    # join_conditions += f'{ table_one+" "+aliases[table_one]+" inner join on "+table_two+" "+aliases[table_two]+" on "+join_condition if join_conditions.find("join") ==-1 else " and inner join "+table_two+" "+aliases[table_two]+" on "+join_condition} \n'
                    check_joins.append(table_one)
                    check_joins.append(table_two)
                    
                if j == final_index-1 and table_one not in check_joins:
                    messages.append(f"No Join condition is defined for table : {table_one}")
        final_query = selected_items+join_conditions

    else : 
        final_query = selected_items + database_name+"."+unique_tables[0]+" as "+ aliases[unique_tables[0]]
        messages = ""

    # uncomment the below lines for in depth analysis 
    # print("No of rows read from excel files = ",rows)
    # print("Final Query : \n", selected_items)
    # print("unique tables : " , unique_tables)
    
    # print("unique tables : " , aliases)
    # print("join conditions : ",join_conditions)
    # print("----------")
    # print(messages)
    # for msg in messages:
    #     print(msg)

    return final_query, messages , unique_tables, aliases