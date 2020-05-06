from utils.adapters import DataAdapter
import pandas as pd
from src.plugins.ESP_MSVP.fetcher import SpainWikiFetcher
from utils.send_email import send_email
import sys

# python function:  src/utils/chek_incoming_data.py  
# DB table=  covid19_schema.staging_epidemiology
# DB fucntion =covid19_schema.covid19_compare_tables(param)
# The function sole purpose is to compare existing data in the epidemiology 
# table against the corresponding set of incoming data ( a subset of the incoming
# data corresponding to all data - newdata). The reason for the comparison is 
# to check if old data had been modified.
# data is fetched user the preexisting fetcher ( fetcher now takes a 0 or 1 
# parameter corresponding to 0:validate the data  and 1: upsert the data) this is 
# a change we have to make to all fetchers. fetcher(0) inserts the incoming 
# data in a staging table ( staging_epidemiology) we then trigger the function .
#  covid19_schema.covid19_compare_tables(param) which hashes the new incoming 
#  set against the preexisting set in table epidemiology.if there is 
#  no difference it calls fetcher with fetcher(1) and upserts othewise it 
#  sends and email to the appropriate contact.

class check_incoming_data():
      

       
    def compare_tables(self):
        db = DataAdapter.get_adapter()
        
        result=db.select_url()
        df = pd.DataFrame(result)
       
        for i in range(len(df)) : 
            url = df.iloc[i, 0]
            source_code=df.iloc[i, 1]
            plugin_name=df.iloc[i, 2]         
                           
            # clean up the staging_epidemiology table
            table_name="staging_epidemiology"
            db.delete_data(table_name)
            
            module_name=getattr(sys.modules[__name__], plugin_name)
            # run fetcher with 0 parameter to insert into the statging epidemiology table                
            fetcher=module_name(db=db)
            fetcher.run(0)                
                        
            
            # compare the data
            compare_result=db.call_db_function(source_code)
            # compare_result=db.callproc('covid19_compare_tables',[source_code])
            if compare_result ==0:
                #update teh table with status
                fetcher.run(1) 
            else:
                message="source {} header has changed".format(plugin_name)
                send_email(source_code,message)             
                
                             
         