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

__all__ = ('PRT_MSDSFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher
from utils.helper import int_or_none

""" 
site-location: https://github.com/dssg-pt/covid19pt-data
COVID19-Portugal Data for Portugal created, maintained and hosted by Data Science for Social Good Portugal
The data source: Portuguese Ministry of Health.
    
"""
logger = logging.getLogger(__name__)


class PRT_MSDSFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'PRT_MSDS'

    def fetch(self):
        url = 'https://raw.githubusercontent.com/dssg-pt/covid19pt-data/master/data.csv'
        logger.debug('Fetching Portugal country&province-level data from PRT_MSDS')
        # drop any dates without associated data using dropna
        return pd.read_csv(url).dropna(thresh=2)

    def run(self):
        data = self.fetch()

        # extract national data and upsert
        country_data = data[['data', 'confirmados', 'obitos', 'recuperados', 'internados', 'internados_uci']].copy()
        for index, record in country_data.iterrows():
            date = datetime.strptime(record['data'], '%d-%m-%Y').strftime('%Y-%m-%d')
            confirmed = int_or_none(record['confirmados'])
            dead = int_or_none(record['obitos'])
            recovered = int_or_none(record['recuperados'])
            admitted = int_or_none(record['internados'])
            icu = int_or_none(record['internados_uci'])

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                 'country': "Portugal",
                'countrycode': 'PRT',
                'gid': ['PRT'],
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,
                'hospitalised': admitted,
                'hospitalised_icu': icu
            }

            self.upsert_data(**upsert_obj)

        ### Manually get province names with matching codes used in the dataset to extract relevant columns
        province_codes = {'ARS Norte': '_arsnorte', 'ARS Centro': '_arscentro', 'ARS Lisboa e Vale do Tejo': '_arslvt',
                          'ARS Alentejo': '_arsalentejo', 'ARS Algarve': '_arsalgarve',
                          'Região Autónoma dos Açores': '_acores', 'Região Autónoma da Madeira': '_madeira',
                          'Overseas': '_estrangeiro'}

        # extract data for each province and upsert
        for province in province_codes:
            province_data = data[
                ['data', 'confirmados' + province_codes[province], 'obitos' + province_codes[province]]].copy()

            for index, record in province_data.iterrows():
                date = datetime.strptime(record['data'], '%d-%m-%Y').strftime('%Y-%m-%d')
                confirmed = int_or_none(record['confirmados' + province_codes[province]])
                dead = int_or_none(record['obitos' + province_codes[province]])

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=province,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True,
                    suppress_exception=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': "Portugal",
                    'countrycode': 'PRT',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'gid': gid,
                    'confirmed': confirmed,
                    'dead': dead
                }

                self.upsert_data(**upsert_obj)
