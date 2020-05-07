import smtplib 
from utils.adapters import DataAdapter
import os

#function sends an email to the contact related to the fetcher 

def send_email(source: str, message: str):
        email = os.getenv('SYS_EMAIL')
        password = os.getenv('SYS_EMAIL_PASS')

        db = DataAdapter.get_adapter()
        destination=db.select_email(source)
        
        s = smtplib.SMTP('smtp.gmail.com', 587) 
        s.starttls() 
        s.login(email, password) 
        s.sendmail("covid19server", destination, message) 
        s.quit()

