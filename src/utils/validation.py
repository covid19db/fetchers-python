from utils.adapters import DataAdapter
from utils.email import send_email

def validate_incoming_data(pname: str):

    db = DataAdapter.get_adapter()
    
    result=db.get_souce_code(pname) 
    source_code=result[0][0]
    compare_result=db.call_db_function_compare(source_code) 
   
    if compare_result[0]==0:
        db.call_db_function_send_data(source_code) 
        return 1
    else:
        message="validation failed for {pname} / {source_code}. please check"
        send_email(source_code,message)
        return 0
