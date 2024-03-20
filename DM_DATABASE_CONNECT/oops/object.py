class Data_migration_object:
    def __init__(self):
        self.environemnt_details:dict = {}
        self.assign_environment_details()

    def assign_environment_details(self):
        environment_file = open('environment.txt','r')

        for line in environment_file.readlines():
            if line.strip() != "" and line.find("#") == - 1:
                key,value = line.split("=")
                key = key.strip()
                value = value.strip()
                self.environemnt_details[key] = value


mig_obj = Data_migration_object()