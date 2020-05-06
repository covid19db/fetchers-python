import requests
from utils.adapter_abstract import AbstractAdapter
import pandas as pd
from utils.send_email import send_email

def check_url(db: AbstractAdapter):
        
        result=db.select_url()
        df = pd.DataFrame(result)
    
        for i in range(len(df)) : 
            url = df.iloc[i, 0]
            source_code=df.iloc[i, 1]
            try:
                page=requests.get(url)
                if page.status_code==200:
                    db.update_url_status('OK',source_code) 
                else:
                    db.update_url_status('DOWN',source_code)
                    send_email(source_code,"the  {} link is down. please check".format(source_code))
            except requests.exceptions.ConnectionError:
                print(f"URL {url} not reachable".format(url)) 
                
        