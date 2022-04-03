from bs4 import BeautifulSoup

import pandas as pd
import datetime
import requests
import zipfile
import io
import os
import time


def get_reports_url(wd, base_url):
    wd.get(base_url)
    # the site loads slowly, so wait until all content is present
    time.sleep(10)

    soup = BeautifulSoup(wd.page_source, "lxml")
    divs = soup.findAll("a")

    for div in divs:
        if div.text == "Pobierz archiwalne dane dla województw":
            voivodeship_zip_url = div['href']
        if div.text == "Pobierz archiwalne dane dla powiatów":
            regions_zip_url = div['href']

    return voivodeship_zip_url, regions_zip_url


def download_reports(zip_url, temp_path):
    os.system(f"rm -rf {temp_path}/*")
    r = requests.get(zip_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(temp_path)


def load_daily_report(path, file_name):
    date = datetime.date(int(file_name[:4]), int(file_name[4:6]), int(file_name[6:8]))
    data_file_path = f"{path}/{file_name}"

    encoding = 'cp1250'
    with open(data_file_path, 'rb') as f:
        f.readline()
        line = f.readline()
        if b'Ca\xc5\x82y kraj' in line:
            encoding = 'utf-8'

    df_data = pd.read_csv(data_file_path, sep=';', decimal=",", encoding=encoding).fillna(0)

    if 'liczba_wszystkich_zakazen' in df_data.columns:
        df_data.rename(columns={'liczba_wszystkich_zakazen': 'liczba_przypadkow'}, inplace=True)
    if 'powiat' in df_data.columns:
        df_data.rename(columns={'powiat': 'powiat_miasto'}, inplace=True)

    return df_data, date


def cumulative(current_cases, previous_day_cases):
    if previous_day_cases is not None:
        cum = float(previous_day_cases)
    else:
        cum = 0

    if current_cases is not None:
        cum = cum + float(current_cases)

    return cum
