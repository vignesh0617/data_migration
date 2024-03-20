from pyrfc import Connection
from oops.object import mig_obj

def get_connection() -> Connection :
    try:
        print("Connecting to S4 system...")
        s4conn = Connection(
                            user=mig_obj.environemnt_details["user2"], 
                            passwd=mig_obj.environemnt_details["password2"], 
                            ashost=mig_obj.environemnt_details["ashost"], 
                            sysnr=mig_obj.environemnt_details["sysnr"], 
                            client=mig_obj.environemnt_details["client"]
                        )
        print("Connected to S4 system Successfully...")
        return s4conn
    except Exception as e :
        print("Can not create a Connection with S4HANA. The error is \n" + e)
