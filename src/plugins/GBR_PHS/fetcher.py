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

# Collecting data from Public Health Scotland via
# 1) DataScienceScotland github account
# 2) ArcGIS account

# Updating Level 1: all Scotland; Level 2: Health Boards; Level 3: Local Authorities


import logging
import pandas as pd
import json
import requests
from datetime import datetime

__all__ = ('ScotlandFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class ScotlandFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'GBR_PHS'

    def fetch_health_board(self):
        datetimeobj = datetime.today()
        attempts = 4
        while attempts > 0:
            try:
                day = datetimeobj.strftime('Y%m%d')
                url = f'https://www.opendata.nhs.scot/dataset/b318bddf-a4dc-4262-971f-0ba329e09b87/resource/2dd8534b-0a6f-4744-9253-9565d62f96c2/download/trend_hb_{day}.csv'
                data = pd.read_csv(url)
                return data
            except:
                datetimeobj = datetimeobj - timedelta(days=1)
                attempts = attempts - 1

    def fetch_national(self):
        url = 'https://raw.githubusercontent.com/DataScienceScotland/COVID-19-Management-Information/master/COVID19' \
              '%20-%20Daily%20Management%20Information%20-%20Scotland%20-%20Deaths.csv'
        deaths_df = pd.read_csv(url, header=0, parse_dates=[0], names=['date', 'deaths'])
        url = 'https://raw.githubusercontent.com/DataScienceScotland/COVID-19-Management-Information/master/COVID19' \
              '%20-%20Daily%20Management%20Information%20-%20Scotland%20-%20Testing.csv'
        testing_df = pd.read_csv(url, header=0, parse_dates=[0], names=['date', 'positive', 'total'], usecols=[0, 2, 3])
        return testing_df.merge(deaths_df, how='outer', on='date')

    def fetch_local_authority(self):
        datetimeobj = datetime.today()
        attempts = 4
        while attempts > 0:
            try:
                day = datetimeobj.strftime('Y%m%d')
                url = f'https://www.opendata.nhs.scot/dataset/b318bddf-a4dc-4262-971f-0ba329e09b87/resource/427f9a25-db22-4014-a3bc-893b68243055/download/trend_ca_{day}.csv'
                data = pd.read_csv(url)
                return data
            except:
                datetimeobj = datetimeobj - timedelta(days=1)
                attempts = attempts - 1

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch_national()

        for index, record in data.iterrows():
            date = record['date']
            confirmedcases = int(record['positive']) if pd.notna(record['positive']) else None
            deaths = int(record['deaths']) if pd.notna(record['deaths']) else None
            tests = int(record['total']) if pd.notna(record['total']) else None

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': 'Scotland',
                'tested': tests,
                'confirmed': confirmedcases,
                'dead': deaths,
                'gid': ['GBR.3_1']
            }

            self.upsert_data(**upsert_obj)

        logger.debug('Fetching health board information')
        logger.warning('GIDs are approximations of health boards by local authorities')
        data = self.fetch_health_board()

        for index, record in data.iterrows():
            date = datetime.strptime(str(record['Date']), '%Y%m%d').strftime('%Y-%m-%d')
            input_adm_area_2 = record['HBName']
            confirmed = record['CumulativePositive']
            deaths = record['CumulativeDeaths']
            tested = confirmed + record['CumulativeNegative']

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1='Scotland',
                input_adm_area_2=input_adm_area_2,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'tested': tested,
                'confirmed': confirmed,
                'dead': deaths,
                'gid': gid
            }

            self.upsert_data(**upsert_obj)

        logger.debug('Fetching local authority information')
        data = self.fetch_local_authority()

        for index, record in data.iterrows():
            date = datetime.strptime(str(record['Date']),'%Y%m%d').strftime('%Y-%m-%d')
            input_adm_area_2 = record['CAName']
            confirmed = record['CumulativePositive']
            deaths = record['CumulativeDeaths']
            tested = confirmed + record['CumulativeNegative']

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1='Scotland',
                input_adm_area_2=input_adm_area_2,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'tested': tested,
                'confirmed': confirmed,
                'dead': deaths,
                'gid': gid
            }

            self.upsert_data(**upsert_obj)

            # upsert this at level three as well for mapping
            upsert_obj['adm_area_3'] = adm_area_2
            upsert_obj['gid'] = [upsert_obj['gid'][0].split('_')[0] + '.1_1']
            self.upsert_data(**upsert_obj)

