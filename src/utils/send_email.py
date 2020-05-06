import smtplib 
from utils.adapters import DataAdapter

#function sends an email to the contact related to the fetcher 

def send_email(source: str, message: str):

        db = DataAdapter.get_adapter()
        destination=db.select_email(source)
        
        s = smtplib.SMTP('smtp.gmail.com', 587) 
        s.starttls() 
        s.login("??????@gmail.com", "????????") 
        s.sendmail("covid19server", destination, message) 
        s.quit()

