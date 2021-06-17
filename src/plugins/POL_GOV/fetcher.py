# Copyright (C) 2020 University of Oxford
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

import logging
import os
from selenium import webdriver
from pandas import DataFrame
from datetime import timedelta

from utils.adapter.abstract_adapter import AbstractAdapter
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

__all__ = ('PolandGovFetcher',)

from .utils import get_reports_url, cumulative, download_reports, load_daily_report

logger = logging.getLogger(__name__)


class PolandGovFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'POL_GOV'
    TEMP_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'temp')

    def __init__(self, data_adapter: AbstractAdapter):
        super().__init__(data_adapter)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        self.wd = webdriver.Chrome(chrome_options=chrome_options)
        self.wd.set_page_load_timeout(300)
        self.wd.set_script_timeout(60)
        self.wd.implicitly_wait(5)

    def cumulative(self, gid: str, date):
        previous_day_date = (date - timedelta(days=1)).strftime('%Y-%m-%d')
        prv_day = self.get_data(source='POL_COVID', gid=gid, date=previous_day_date)
        if not prv_day:
            print(f"Unable to find data for: {date}, {gid}")
            return False, 0, 0
        return True, prv_day['confirmed'], prv_day['dead']

    def update_cases(self, date: str, df: DataFrame, data_type: str):
        for index, row in df.iterrows():
            region = row['powiat_miasto'] if 'powiat_miasto' in row.index else None
            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=row['wojewodztwo'],
                input_adm_area_2=region,
                input_adm_area_3=None,
                return_original_if_failure=False
            )

            print(
                f"{date} - {row['wojewodztwo']}, {region} -> {adm_area_1}, {adm_area_2}, {adm_area_3}, {gid}")

            upsert_obj = {
                'source': self.SOURCE,
                'date': date.strftime('%Y-%m-%d'),
                'country': 'Poland',
                'countrycode': 'POL',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid
            }

            if data_type == 'confirmed':
                upsert_obj['confirmed'] = float(row['liczba_przypadkow'])
            elif data_type == 'deaths':
                upsert_obj['dead'] = float(row['zgony'])
            else:
                raise Exception('Data type not supported!')

            self.upsert_data(**upsert_obj)

            # Ignore deaths
            if data_type == 'deaths':
                continue

            # Cumulative data, stored in POL_COVID
            success, previous_day_confirmed, previous_day_dead = self.cumulative(gid, date)

            upsert_obj['source'] = 'POL_COVID'
            if data_type == 'confirmed':
                upsert_obj['confirmed'] = cumulative(row['liczba_przypadkow'], previous_day_confirmed)
            elif data_type == 'deaths':
                upsert_obj['dead'] = cumulative(row['zgony'], previous_day_dead)
            else:
                raise Exception('Data type not supported!')

            self.upsert_data(**upsert_obj)

    def run(self):
        base_url = "https://wojewodztwa-rcb-gis.hub.arcgis.com/pages/dane-do-pobrania"
        voivodeship_zip_url, regions_zip_url = get_reports_url(self.wd, base_url)

        # Wojewodztwa
        voivodeship_temp_path = os.path.join(self.TEMP_PATH, "voivodeship")
        download_reports(voivodeship_zip_url, voivodeship_temp_path)

        file_list = sorted(os.listdir(voivodeship_temp_path))
        for file_name in file_list:
            if file_name.endswith('csv'):
                df_data, date = load_daily_report(voivodeship_temp_path, file_name)

                self.update_cases(date,
                                  df_data[['wojewodztwo', 'liczba_przypadkow']],
                                  'confirmed')

                self.update_cases(date,
                                  df_data[['wojewodztwo', 'zgony']],
                                  'deaths')

        # Powiaty
        regions_temp_path = os.path.join(self.TEMP_PATH, "regions")
        download_reports(regions_zip_url, regions_temp_path)

        file_list = sorted(os.listdir(regions_temp_path))
        for file_name in file_list:
            if file_name.endswith('csv'):
                df_data, date = load_daily_report(regions_temp_path, file_name)

                self.update_cases(date,
                                  df_data[['wojewodztwo', 'powiat_miasto', 'liczba_przypadkow']],
                                  'confirmed')

                self.update_cases(date,
                                  df_data[['wojewodztwo', 'powiat_miasto', 'zgony']],
                                  'deaths')

        os.system(f"rm -rf {self.TEMP_PATH}")
