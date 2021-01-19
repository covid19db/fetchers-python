import requests
from bs4 import BeautifulSoup

from io import StringIO
import pandas as pd
import numpy as np
import datetime
import re

file_name_regexp_1 = re.compile(
    r".+filename.+'(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})(?P<min>\d{2})(?P<sec>\d{2}).+csv")
file_name_regexp_2 = re.compile(r".+filename.+(?P<day>\d{2})(?P<month>\d{2})(?P<year>\d{4})_.+csv")
file_name_regexp_3 = re.compile(r".+filename.+(?P<day>\d{2})_(?P<month>\d{2})_(?P<year>\d{2,4}).+csv")


def get_daily_report(url):
    req = requests.get(url)
    req.encoding = 'cp1250'
    content_disposition = req.headers['content-disposition']

    regex_list = [file_name_regexp_1, file_name_regexp_2, file_name_regexp_3]
    for regex in regex_list:
        s = regex.match(content_disposition)
        if s:
            day, month = int(s['day']), int(s['month'])
            year = int(s['year']) if int(s['year']) > 2000 else int(s['year']) + 2000
            break
    else:
        raise NotImplemented('Unknown date format')

    date = datetime.date(int(year) if int(year) > 2000 else int(year) + 2000, int(month), int(day))
    df_data = pd.read_csv(StringIO(req.text), sep=';', decimal=",").replace({np.nan: None})
    if 'wojewodztwo' in df_data.columns:
        df_data.rename({'wojewodztwo': 'Województwo'}, axis=1, inplace=True)
    if 'powiat_miasto' in df_data.columns:
        df_data.rename({'powiat_miasto': 'Powiat/Miasto'}, axis=1, inplace=True)
    if 'liczba_przypadkow' in df_data.columns:
        df_data.rename({'liczba_przypadkow': 'Liczba'}, axis=1, inplace=True)
    if 'zgony' in df_data.columns:
        df_data.rename({'zgony': 'Wszystkie przypadki śmiertelne'}, axis=1, inplace=True)
    if 'Liczba przypadków' in df_data.columns:
        df_data.rename({'Liczba przypadków': 'Liczba'}, axis=1, inplace=True)
    if 'Zgony' in df_data.columns:
        df_data.rename({'Zgony': 'Wszystkie przypadki śmiertelne'}, axis=1, inplace=True)

    return df_data, date


def get_regional_report_urls(base_url):
    page = requests.get(base_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    divs = soup.findAll("a", {"class": "file-download"})
    return [f"https://www.gov.pl{div['href']}" for div in reversed(divs)]


def get_recent_regional_report_url(base_url):
    page = requests.get(base_url)

    soup = BeautifulSoup(page.content, 'html.parser')
    divs = soup.findAll("a", {"class": "file-download"})
    for div in divs:
        if div['href']:
            return div['href']


def cumulative(current_cases, previous_day_cases):
    if previous_day_cases is not None:
        cum = previous_day_cases
    else:
        cum = 0

    if current_cases is not None:
        cum = cum + current_cases

    return cum
