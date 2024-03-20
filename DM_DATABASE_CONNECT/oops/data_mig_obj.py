import pandas as pd
# from databases.MySQL.connection import get_data_as_data_frame

class Joins:
    def __init__(self):
        self.join_conditions_df_excel = pd.read_excel("Join_conditions.xlsx",sheet_name="join_conditions")
        # self.join_conditions_df_database = get_data_as_data_frame(sql_query="select * from join_conditions")

    def get_join_condition_from_excel(self,table_one:str,table_two:str):
        first_df = self.join_conditions_df_excel[self.join_conditions_df_excel["Table_A"].apply(lambda x : x.lower() == table_one.lower()) | self.join_conditions_df_excel["Table_A"].apply(lambda x : x.lower() == table_two.lower())]
        second_df = first_df[first_df['Table_B'].apply(lambda x : x.lower() == table_one.lower()) | first_df['Table_B'].apply(lambda x : x.lower() == table_two.lower())]
        if len(second_df)==0:
            return f"Error : Condition is not defined for Tables : \n\t1) {table_one} & \n\t2) {table_two}"
        
        return second_df["Join_Condition"][second_df.index[0]]
    
    # def get_join_condition_from_database(self,table_one:str,table_two:str):
    #     first_df = self.join_conditions_df_database[self.join_conditions_df_database["TABLE_A"].apply(lambda x : x.lower() == table_one.lower()) | self.join_conditions_df_database["TABLE_A"].apply(lambda x : x.lower() == table_two.lower())]
    #     second_df = first_df[first_df['TABLE_B'].apply(lambda x : x.lower() == table_one.lower()) | first_df['TABLE_B'].apply(lambda x : x.lower() == table_two.lower())]
    #     if len(second_df)==0:
    #         return f"Error : Condition is not defined for Tables : \n\t1) {table_one} & \n\t2) {table_two}"
        
    #     return second_df["JOIN_CONDITION"][second_df.index[0]]
    

class MigrationObj:
    def __init__(self,environment_file_name):
        self.environemnt_file_name:str = environment_file_name

        # self.joins_obj = Joins()

        self.environemnt_details:dict = {}
        self.assign_environment_details()

        self.sheet_names :list[str] = []

        self.extraction_rules : pd = None
        
        self.excel_data:dict[pd.DataFrame] = {}
        self.assign_excel_data()


    def assign_environment_details(self):
        environment_file = open(self.environemnt_file_name,'r')

        for line in environment_file.readlines():
            if line.strip() != "" and line.find("#") == - 1:
                key,value = line.split("=")
                key = key.strip()
                value = value.strip()
                self.environemnt_details[key] = value

    def create_range_for_rows_to_skip(self,in_range_values:str) -> list[int]:
        final_range = []
        in_range_list = in_range_values.split(",")
        
        for temp_range in in_range_list:

            

            #if the type casting can not be done, it indicates we have passed in a ":"  separated value, then except part will run
            try:
                value = int(temp_range)
                final_range.append(value)

            # here we convert the ":" sepated value to a separate list
            except:
                start_number,end_number = temp_range.split(":")
                values = list(range(int(start_number),int(end_number)+1))
                final_range.extend(values)

        return final_range


    def assign_excel_data(self):
        self.sheet_names = self.environemnt_details["sheet_names"].split(";")
        no_of_sheets = len(self.sheet_names)
        for i in range(no_of_sheets):
            sheet_name = self.sheet_names[i]

            rows_to_skip = self.create_range_for_rows_to_skip(self.environemnt_details["no_of_rows_to_skip"].split(";")[i])

            # rows_to_skip = [int(x) for x in self.environemnt_details["no_of_rows_to_skip"].split(";")[i].split(",")]
            
            cols_to_read = self.environemnt_details["cols_to_read"].split(";")[i]
            excel_data = pd.read_excel(io=self.environemnt_details["excel_file_location"],sheet_name=sheet_name,skiprows=rows_to_skip,usecols=cols_to_read)
            
            
            # uncomment this if needed
            # thresh_value = int(self.environemnt_details["min_no_of_non_empty_cols"].split(";")[i])
            # self.excel_data[sheet_name] = excel_data.dropna(thresh=thresh_value)

            self.excel_data[sheet_name] = excel_data

        self.extraction_rules = pd.read_excel(
            io=self.environemnt_details["excel_file_location"],
            sheet_name=self.environemnt_details["extraction_rules_sheet"],
            skiprows=self.create_range_for_rows_to_skip(self.environemnt_details["rows_to_skip_for_extraction_rules"]),
            usecols=self.environemnt_details["cols_to_read_for_extraction_rules"]
        ) 

       