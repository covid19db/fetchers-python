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
from pandas import DataFrame
from datetime import timedelta

from utils.config import config
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

__all__ = ('PolandGovFetcher',)

from .utils import get_daily_report, get_regional_report_urls, get_recent_regional_report_url, cumulative

logger = logging.getLogger(__name__)


class PolandGovFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'POL_GOV'

    def cumulative(self, gid: str, date):
        previous_day_date = (date - timedelta(days=1)).strftime('%Y-%m-%d')
        prv_day = self.get_data(source='POL_COVID', gid=gid, date=previous_day_date)
        if not prv_day:
            print(f"Unable to find data for: {date}, {gid}")
            return False, 0, 0
        return True, prv_day['confirmed'], prv_day['dead']

    def update_cases(self, date: str, df: DataFrame, data_type: str):
        for index, row in df.iterrows():
            region = row['Powiat/Miasto'] if 'Powiat/Miasto' in row.index else None
            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=row['Województwo'],
                input_adm_area_2=region,
                input_adm_area_3=None,
                return_original_if_failure=False
            )

            print(
                f"{date} - {row['Województwo']}, {region} -> {adm_area_1}, {adm_area_2}, {adm_area_3}, {gid}")

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
                upsert_obj['confirmed'] = row['Liczba']
            elif data_type == 'deaths':
                upsert_obj['dead'] = row['Wszystkie przypadki śmiertelne']
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
                upsert_obj['confirmed'] = cumulative(row['Liczba'], previous_day_confirmed)
            elif data_type == 'deaths':
                upsert_obj['dead'] = cumulative(row['Wszystkie przypadki śmiertelne'], previous_day_dead)
            else:
                raise Exception('Data type not supported!')

            self.upsert_data(**upsert_obj)

    def run(self):
        # Wojewodztwa
        report_urls = get_regional_report_urls("https://www.gov.pl/web/koronawirus/pliki-archiwalne-wojewodztwa")
        report_urls.append(
            get_recent_regional_report_url("https://www.gov.pl/web/koronawirus/wykaz-zarazen-koronawirusem-sars-cov-2"))
        for url in report_urls:
            df_data, date = get_daily_report(url)

            self.update_cases(date,
                              df_data[['Województwo', 'Liczba']],
                              'confirmed')

            self.update_cases(date,
                              df_data[['Województwo', 'Wszystkie przypadki śmiertelne']],
                              'deaths')

        # Powiaty
        report_urls = get_regional_report_urls("https://www.gov.pl/web/koronawirus/pliki-archiwalne-powiaty")
        report_urls.append(get_recent_regional_report_url(
            "https://www.gov.pl/web/koronawirus/mapa-zarazen-koronawirusem-sars-cov-2-powiaty"))
        for url in report_urls:
            df_data, date = get_daily_report(url)

            self.update_cases(date,
                              df_data[['Województwo', 'Powiat/Miasto', 'Liczba']],
                              'confirmed')

            self.update_cases(date,
                              df_data[['Województwo', 'Powiat/Miasto', 'Wszystkie przypadki śmiertelne']],
                              'deaths')
