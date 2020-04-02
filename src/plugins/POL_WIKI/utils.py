import re
import requests
import pandas as pd
from typing import List
from bs4 import Tag
from bs4 import BeautifulSoup
from collections import OrderedDict


def correct_cell_value(value: str):
    # Remove new lines characters and square bracket
    value = value.text.strip().replace('\n', '')
    value = re.sub(r'\[.*?\]', '', value)
    return value


def validate_row(row: str):
    # Skip rows with small amount of columns and with text
    if not row or len(row) < 2:
        return False
    if 'Total' in row[0]:
        return False
    return True


def to_number(item: str):
    # Remove coma from numbers and convert to int
    if isinstance(item, str):
        item = item.replace(',', '')
    return int(item or 0)


def fetch_html_tables_from_wiki(url: str):
    website_content = requests.get(url)
    soup = BeautifulSoup(website_content.text, 'lxml')
    return soup.find_all("table", {"class": "wikitable"})


def html_table_to_df(table: Tag):
    header = []
    for th in table.findAll('th'):
        key = th.get_text().replace('\n', '')
        key = re.sub(r'\[.*?\]', '', key)
        header.append(key)

    if 'Date' not in header[0]:
        header.pop(0)

    table_rows = table.find_all('tr')
    res = []
    for tr in table_rows:
        td = tr.find_all('td')
        row = [correct_cell_value(cell) for cell in td]
        if validate_row(row):
            res.append(OrderedDict(zip(header, row)))
    df = pd.DataFrame(res).fillna(0)
    date_column = df.columns[0]
    df[date_column] = pd.to_datetime(df[date_column], format='%d %B %Y')
    df.sort_values(by=[date_column], inplace=True, ascending=True)
    df.rename(columns={date_column: 'Date'}, inplace=True)
    return df


def extract_data_table(data_tables: List[str], text: str):
    for data_table in data_tables:
        if data_table.find(text=re.compile(text)):
            return html_table_to_df(data_table)
    return pd.DataFrame()
