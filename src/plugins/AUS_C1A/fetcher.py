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
# The Real-time COVID-19 Status in Australia
# https://github.com/covid-19-au/covid-19-au.github.io
#

import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('AustraliaC1AFetcher',)

logger = logging.getLogger(__name__)


class AustraliaC1AFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'AUS_C1A'

    def fetch(self, category):
        return pd.read_json(f'https://raw.githubusercontent.com/covid-19-au/covid-19-au.github.io/'
                            f'prod/src/data/{category}.json', orient='index')

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch('country')

        for index, record in data.iterrows():
            # confirmed, recovered, deaths, active, tested, in_hospital, in_icu
            date = index.strftime('%Y-%m-%d')
            confirmed = int(record[0])
            recovered = int(record[1])
            deaths = int(record[2])
            tested = int(record[4]) if pd.notna(record[4]) else None
            in_hospital = int(record[5]) if pd.notna(record[5]) else None
            in_icu = int(record[6]) if pd.notna(record[6]) else None

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'Australia',
                'countrycode': 'AUS',
                'gid': ['AUS'],
                'tested': tested,
                'confirmed': confirmed,
                'dead': deaths,
                'recovered': recovered,
                'hospitalised': in_hospital,
                'hospitalised_icu': in_icu
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug('Fetching regional information')
        data = self.fetch('state')

        for index, row in data.iterrows():
            date = index.strftime('%Y-%m-%d')
            for state, record in row.items():
                # confirmed, deaths, recovered, tested, in_hospital, in_icu
                length = len(record)
                confirmed = int(record[0])
                deaths = int(record[1]) if length > 1 else None
                recovered = int(record[2]) if length > 2 else None
                tested = int(record[3]) if length > 3 else None
                in_hospital = int(record[4]) if length > 4 else None
                in_icu = int(record[5]) if length > 5 else None

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=state,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': 'AUS_C1A',
                    'date': date,
                    'country': 'Australia',
                    'countrycode': 'AUS',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'adm_area_3': adm_area_3,
                    'gid': gid,
                    'tested': tested,
                    'confirmed': confirmed,
                    'dead': deaths,
                    'recovered': recovered,
                    'hospitalised': in_hospital,
                    'hospitalised_icu': in_icu
                }
                self.db.upsert_epidemiology_data(**upsert_obj)
