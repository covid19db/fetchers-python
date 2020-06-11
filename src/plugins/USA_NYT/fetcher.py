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

#
# USA county-wise data as collected by the New York Times
# See: https://github.com/nytimes/covid-19-data
#

import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('UnitedStatesNYTFetcher',)

logger = logging.getLogger(__name__)


class UnitedStatesNYTFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'USA_NYT'

    def fetch(self, category):
        return pd.read_csv(f'https://raw.githubusercontent.com/nytimes/covid-19-data/'
                           f'master/{category}.csv')

    def run(self):
        logger.debug('Going to fetch the NY Times US counties')
        data = self.fetch('us-counties')

        for index, record in data.iterrows():
            # date,county,state,fips,cases,deaths
            date = record[0]
            county = record[1]
            state = record[2]
            cases = int(record[4])
            deaths = int(record[5])

            # Skip "Unknown" counties and a few cities
            if county in ('Unknown', 'Baltimore', 'Kansas City', 'St. Louis city'):
                continue
            if state == 'Virginia' and county in ('Richmond', 'Franklin city'):
                continue

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='USA',
                input_adm_area_1=state,
                input_adm_area_2=county,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United States',
                'countrycode': 'USA',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'confirmed': cases,
                'dead': deaths,
                'gid': gid
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug('Going to fetch the NY Times US States')
        data = self.fetch('us-states')

        for index, record in data.iterrows():
            # date,state,fips,cases,deaths
            date = record[0]
            state = record[1]
            cases = int(record[3])
            deaths = int(record[4])

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='USA',
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United States',
                'countrycode': 'USA',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'confirmed': cases,
                'dead': deaths,
                'gid': gid
            }
            self.db.upsert_epidemiology_data(**upsert_obj)
