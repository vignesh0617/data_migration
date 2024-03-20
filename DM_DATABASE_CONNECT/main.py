# from extraction_module.extract_source_data import extract_source
from databases.MySQL.sql_script_generator.filter_statement import get_where_condition
from databases.MySQL.sql_script_generator.select_statement import get_select_query
from extraction_module.extract_source_data import extract_data_to_extraction_db
from oops.individual_migration_objects import material_obj
from oops.object import mig_obj
# from staging_module.staging import load_table_to_staging_layer


#step 1 : Loading the table data from s4hana to mysql staging layer
# load_table_to_staging_layer(table_name="MARA")


# STEP 1 : EXTRACTION MODULE
# TEST CASE : FOR TABLE MARA AND COUNTRY GERMANY
print("----------")
# print(extract_source(table_name = "marc", country= "Germany"))
print("----------")


index = 0
for key,value in material_obj.excel_data.items():
    # value.to_csv(path_or_buf = f'{data_migration_obj.environemnt_details["csv_output_location"]}\\\\{key}.txt',sep="\t")    

    select_statement , warning_messages , unique_tables , table_aliases = get_select_query(
                   data_frame = value, 
                   source_table_number = int(material_obj.environemnt_details["source_table_col_number"].split(";")[index]), 
                   source_field_number = int(material_obj.environemnt_details["source_field_col_number"].split(";")[index]),
                   database_name= mig_obj.environemnt_details["loading_database_name"]
                )
    index+=1

    where_condition = get_where_condition(table_names=unique_tables,
                                          alisas_dict= table_aliases,
                                          country="Germany" , df = material_obj.extraction_rules)

    final_query = select_statement+where_condition

    print("====================")
    print(f"Final Query for sheet {key}:","\n",f"{final_query}") 
    
    extract_data_to_extraction_db(staging_table_name="STG_"+str(index),extraction_database_name=mig_obj.environemnt_details["extraction_database_name"],sql_query=final_query)
    if len(warning_messages) > 0 :
        print("----------------")
        print("Warning : The Query will not work. Because \n")
        for msg in warning_messages:
            print(msg)
        print("\nAdd the join conditions for the above mentioned tables then run this program again")
        print("------------")
    



