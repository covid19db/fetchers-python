import pandas as pd
import re
import os
import smtplib


def send_email(src_code: str, message: str):
    emails_file_path = os.path.join(os.path.dirname(__file__), "..", "data", "emails.csv")

    if os.path.isfile(emails_file_path):
        df = pd.read_csv(emails_file_path, encoding='utf-8')

        email1 = df.loc[df['source_code'] == src_code, 'email_curator_1'].iloc[0]
        email2 = df.loc[df['source_code'] == src_code, 'email_curator_2'].iloc[0]
        email3 = df.loc[df['source_code'] == src_code, 'email_curator_3'].iloc[0]
        email4 = df.loc[df['source_code'] == src_code, 'email_curator_4'].iloc[0]

        emails = [email1, email2, email3, email4]
        destination = [email for email in emails if validate_address(email)]

        email = os.getenv('SYS_EMAIL')
        password = os.getenv('SYS_EMAIL_PASS')
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(email, password)
        s.sendmail("covid19db", destination, message)
        s.quit()


def isNaN(string: str) -> str:
    return string != string


def validate_address(email: str) -> bool:
    if not isNaN(email):
        pattern = '^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$'
        return not isNaN(email) and re.match(pattern, email) != None
