import pandas as pd
import re
import os
import smtplib 


def send_email(src_code: str, message: str):
    # change file location to a  directory  on the local server
    if os.path.isfile('./utils/email.csv'):
        df=pd.read_csv("./utils/email.csv",encoding='utf-8')
                
        email1=df.loc[df['source_code'] == src_code, 'email_curator_1'].iloc[0]
        email2=df.loc[df['source_code'] == src_code, 'email_curator_2'].iloc[0]
        email3=df.loc[df['source_code'] == src_code, 'email_curator_3'].iloc[0]
        email4=df.loc[df['source_code'] == src_code, 'email_curator_4'].iloc[0]
        
        destination = list()
        if validate_address(email1):
            destination.append(email1)
        if validate_address(email2):
            destination.append(email2)
        if validate_address(email3):
            destination.append(email3)
        if validate_address(email4):
            destination.append(email4)
            
        # email = os.getenv('SYS_EMAIL')
        # password = os.getenv('SYS_EMAIL_PASS')
        email="oxford.covid19.db@gmail.com"
        password="covid123"
        
        s = smtplib.SMTP('smtp.gmail.com', 587) 
        s.starttls() 
        s.login(email, password) 
        s.sendmail("covid19db", destination, message) 
        s.quit()
            
    
  
def isNaN(string):
    return string != string

def validate_address(email):
     if not isNaN(email) :        
        match = re.match('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$', email)
        if match != None:
            return 1
        else:
            return 0
     else:
        return 0
