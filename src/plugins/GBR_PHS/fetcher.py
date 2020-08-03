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
        url = 'https://raw.githubusercontent.com/DataScienceScotland/COVID-19-Management-Information/master/COVID19' \
              '%20-%20Daily%20Management%20Information%20-%20Scottish%20Health%20Boards%20-%20Cumulative%20cases.csv'
        return pd.read_csv(url, parse_dates=[0], na_values='*')

    def fetch_national(self):
        url = 'https://raw.githubusercontent.com/DataScienceScotland/COVID-19-Management-Information/master/COVID19' \
              '%20-%20Daily%20Management%20Information%20-%20Scotland%20-%20Deaths.csv'
        deaths_df = pd.read_csv(url, header=0, parse_dates=[0], names=['date', 'deaths'])
        url = 'https://raw.githubusercontent.com/DataScienceScotland/COVID-19-Management-Information/master/COVID19' \
              '%20-%20Daily%20Management%20Information%20-%20Scotland%20-%20Testing.csv'
        testing_df = pd.read_csv(url, header=0, parse_dates=[0], names=['date', 'positive', 'total'], usecols=[0, 2, 3])
        return testing_df.merge(deaths_df, how='outer', on='date')

    def fetch_local_authority_date(self):
        url = 'https://services5.arcgis.com/fCRrQtNvX2pM5zRG/ArcGIS/rest/services/Scottish_Covid_Cases_and_Deaths' \
              '/FeatureServer/1?f=pjson'
        timestamp = requests.get(url).json().get('editingInfo').get('lastEditDate')
        timestamp = timestamp // 1000
        date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        return date

    def fetch_local_authority_data(self):
        url = 'https://services5.arcgis.com/fCRrQtNvX2pM5zRG/arcgis/rest/services/Scottish_Covid_Cases_and_Deaths' \
              '/FeatureServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        data = requests.get(url).json().get('features')
        df = pd.DataFrame(columns=['lau', 'confirmedcases'])
        for element in data:
            lau = element.get('attributes').get('Name')
            confirmedcases = element.get('attributes').get('TotalCases')
            df = df.append({'lau': lau, 'confirmedcases': confirmedcases}, ignore_index=True)
        return df

    def fetch_local_authority_date_and_data(self):

        date = self.fetch_local_authority_date()
        data = self.fetch_local_authority_data()

        # check date again to make sure the data didn't update after the date-check
        if date != datetime.today().strftime('%Y-%m-%d'):
            date_1 = self.fetch_local_authority_date()
            if date != date_1:
                date = date_1
                data = self.fetch_local_authority_data()

        return date, data

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
            # date in first column, each health board in its own column
            date = record[0]
            for area in record.index[1:]:
                totalcases = int(record[area]) if pd.notna(record[area]) else None

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1='Scotland',
                    input_adm_area_2=area,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'United Kingdom',
                    'countrycode': 'GBR',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'confirmed': totalcases,
                    'gid': gid
                }

                self.upsert_data(**upsert_obj)

                # repeat this at level 3 for mapping consistence

                upsert_obj['adm_area_3'] = adm_area_2
                self.upsert_data(**upsert_obj)

        '''
        TURNED OFF PENDING ALTERNATIVE DATA SOURCE
        NO DATA AT THIS LOCATION ANY MORE

        logger.debug('Fetching local authority information')
        date, data = self.fetch_local_authority_date_and_data()

        for index, record in data.iterrows():
            # date in first column, each health board in its own column

            area = record['lau']
            totalcases = int(record['confirmedcases']) if pd.notna(record['confirmedcases']) else None

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1='Scotland',
                input_adm_area_2=area,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'confirmed': totalcases,
                'gid': gid
            }

            self.upsert_data(**upsert_obj)
            '''
