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

from datetime import datetime
import logging
import pandas as pd
import math

__all__ = ('ZAF_DSFSIFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

""" 
    site-location: https://github.com/dsfsi/covid19za
    
    COVID19_ZA Data for South Africa created, maintained and hosted by Data Science for Social Impact research group,
    led by Dr. Vukosi Marivate, at the University of Pretoria. 
    
    Their data sources include: NICD - South Africa; Department of Health - South Africa; 
                                South African Government Media Statements;
                                National Department of Health Data Dictionary;
                                MedPages; Statistics South Africa
"""
logger = logging.getLogger(__name__)


class ZAF_DSFSIFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'ZAF_DSFSI'

    @staticmethod
    def int_parser(x):
        return int(x) if not math.isnan(x) else None

    def run(self):

        # The source refers to the provinces as follows:
        province_columns = ['EC', 'FS', 'GP', 'KZN', 'LP', 'MP', 'NC', 'NW', 'WC', 'total']

        # Collect countrywide testing data
        countrywide_df = pd.read_csv(
            'https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_testing.csv')
        # Collect various provincial data
        confirmed_df = pd.read_csv(
            'https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_provincial_cumulative_timeline_confirmed.csv')
        recovered_df = pd.read_csv(
            'https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_provincial_cumulative_timeline_recoveries.csv')
        dead_df = pd.read_csv(
            'https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_provincial_cumulative_timeline_deaths.csv')
        logger.debug('Fetching South African data from ZAF_DSFSI')

        # For each province, create a DataFrame with relevant parts of the three data sets
        province_columns = ['EC', 'FS', 'GP', 'KZN', 'LP', 'MP', 'NC', 'NW', 'WC', 'total']

        data = dict()
        for province in province_columns:
            confirmed = confirmed_df[['date', province]].rename(columns={province: 'confirmed'})
            recovered = recovered_df[['date', province]].rename(columns={province: 'recovered'})
            dead = dead_df[['date', province]].rename(columns={province: 'dead'})
            df = confirmed.merge(recovered, on='date').merge(dead, on='date')
            # convert the date format
            df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%d-%m-%Y').strftime('%Y-%m-%d'))
            data[province] = df

        # For the national data, we can add more information from countrywide_df
        testing_df = countrywide_df[['date', 'cumulative_tests', 'hospitalisation', 'critical_icu']]
        testing_df['date'] = testing_df['date'].apply(lambda x: datetime.strptime(x, '%d-%m-%Y').strftime('%Y-%m-%d'))
        data['total'] = data['total'].merge(testing_df, on='date')

        # upload all the provincial data
        for province in province_columns:
            # skip national data for later
            if province == 'total':
                continue
            for index, record in data[province].iterrows():
                confirmed = self.int_parser(record['confirmed'])
                recovered = self.int_parser(record['recovered'])
                dead = self.int_parser(record['dead'])

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=province,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': record['date'],
                    'country': "South Africa",
                    'countrycode': 'ZAF',
                    'gid': gid,
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'confirmed': confirmed,
                    'dead': dead,
                    'recovered': recovered
                }

                self.upsert_data(**upsert_obj)

        # upload all the national data
        for index, record in data['total'].iterrows():
            confirmed = self.int_parser(record['confirmed'])
            recovered = self.int_parser(record['recovered'])
            dead = self.int_parser(record['dead'])
            tested = self.int_parser(record['cumulative_tests'])
            hospitalised = self.int_parser(record['hospitalisation'])
            hospitalised_icu = self.int_parser(record['critical_icu'])

            upsert_obj = {
                'source': self.SOURCE,
                'date': record['date'],
                'country': "South Africa",
                'countrycode': 'ZAF',
                'gid': ['ZAF'],
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,
                'tested': tested,
                'hospitalised': hospitalised,
                'hospitalised_icu': hospitalised_icu
            }

            self.upsert_data(**upsert_obj)
