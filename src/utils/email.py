# Copyright University of Oxford 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import os
import logging
import smtplib
import itertools
import pandas as pd
from email.message import EmailMessage

from utils.config import config

EMAIL_REGEX = re.compile(r"^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$")

logger = logging.getLogger(__name__)


def validate_address(email: str) -> bool:
    return isinstance(email, str) and re.match(EMAIL_REGEX, email)


def send_email(src_code: str, message: str):
    emails_file_path = os.path.join(os.path.dirname(__file__), "..", "data", "emails.csv")

    if not os.path.isfile(emails_file_path):
        logger.error(f"Unable to send notification email, file: {emails_file_path} doesn't exist")
        return

    df = pd.read_csv(emails_file_path, encoding='utf-8')
    df_recipients = df[(df.source_code == src_code) | (df.source_code == '*')]
    emails_list = df_recipients[
        ['email_curator_1', 'email_curator_2', 'email_curator_3', 'email_curator_4']
    ].values.tolist()
    emails = set(itertools.chain.from_iterable(emails_list))
    receivers = [email for email in emails if validate_address(email)]

    msg = EmailMessage()
    msg['Subject'] = f'Covid19db fetchers-python validation error'
    msg['From'] = "covid19db.org"
    msg['To'] = receivers
    msg.set_content(message)

    with smtplib.SMTP('smtp.gmail.com', 587) as s:
        s.starttls()
        s.login(config.SYS_EMAIL, config.SYS_EMAIL_PASS)
        s.send_message(msg)
        logger.info(f'Email successfully sent to: {receivers}')
