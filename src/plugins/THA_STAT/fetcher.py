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

#
# Covid-19 Infected Situation Reports
# https://covid19.th-stat.com/en/api
#

import logging
import requests
import pandas as pd
from datetime import datetime
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('ThailandSTATFetcher',)

logger = logging.getLogger(__name__)


class ThailandSTATFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'THA_STAT'

    def fetch(self, category):
        return requests.get(f'https://covid19.th-stat.com/api/open/{category}').json()

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch('timeline')

        for record in data['Data']:
            upsert_obj = {
                'source': self.SOURCE,
                'date': datetime.strptime(record['Date'], '%m/%d/%Y').strftime('%Y-%m-%d'),
                'country': 'Thailand',
                'countrycode': 'THA',
                'gid': ['THA'],
                'confirmed': int(record['Confirmed']),
                'dead': int(record['Deaths']),
                'recovered': int(record['Recovered']),
                'hospitalised': int(record['Hospitalized'])
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug('Fetching regional information')
        data = self.fetch('cases')

        # Get cumulative counts from the cross table of dates and provinces
        df = pd.DataFrame(data['Data'], columns=['ConfirmDate', 'ProvinceEn'])
        crosstabsum = pd.crosstab(df.ConfirmDate.apply(lambda d: d[:10]), df.ProvinceEn) \
            .sort_index() \
            .cumsum()

        for confirmdate, row in crosstabsum.iterrows():
            for provinceen, confirmed in row.items():
                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=provinceen,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': confirmdate,
                    'country': 'Thailand',
                    'countrycode': 'THA',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'adm_area_3': adm_area_3,
                    'gid': gid,
                    'confirmed': int(confirmed)
                }
                self.db.upsert_epidemiology_data(**upsert_obj)
