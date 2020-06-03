import re
import os
import smtplib
import itertools
import pandas as pd

from utils.config import config

EMAIL_REGEX = re.compile(r"^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$")


def validate_address(email: str) -> bool:
    return isinstance(email, str) and re.match(EMAIL_REGEX, email)


def send_email(src_code: str, message: str):
    emails_file_path = os.path.join(os.path.dirname(__file__), "..", "data", "emails.csv")

    if os.path.isfile(emails_file_path):
        df = pd.read_csv(emails_file_path, encoding='utf-8')

        df_recipients = df[(df.source_code == src_code) | (df.source_code == '*')]
        emails_list = df_recipients[
            ['email_curator_1', 'email_curator_2', 'email_curator_3', 'email_curator_4']
        ].values.tolist()
        emails = set(itertools.chain.from_iterable(emails_list))
        destination = [email for email in emails if validate_address(email)]

        email = config.SYS_EMAIL
        password = config.SYS_EMAIL_PASS
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(email, password)
        s.sendmail("covid19db", destination, message)
        s.quit()
