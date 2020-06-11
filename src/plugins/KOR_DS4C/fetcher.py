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
# DS4C: Data Science for COVID-19 in South Korea
# https://github.com/jihoo-kim/Data-Science-for-COVID-19
#

import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('SouthKoreaDS4CFetcher',)

logger = logging.getLogger(__name__)


class SouthKoreaDS4CFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'KOR_DS4C'

    def fetch(self, category):
        return pd.read_csv(f'https://raw.githubusercontent.com/jihoo-kim/'
                           f'Data-Science-for-COVID-19/master/dataset/Time/{category}.csv')

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch('Time')

        for index, record in data.iterrows():
            # date, time, test, negative, confirmed, released, deceased
            date = record[0]
            test = int(record[2])
            confirmed = int(record[4])
            released = int(record[5])
            deceased = int(record[6])

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'South Korea',
                'countrycode': 'KOR',
                'gid': ['KOR'],
                'tested': test,
                'confirmed': confirmed,
                'dead': deceased,
                'recovered': released
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug('Fetching regional information')
        data = self.fetch('TimeProvince')

        for index, record in data.iterrows():
            # date, time, province, confirmed, released, deceased
            date = record[0]
            province = record[2]
            confirmed = int(record[3])
            released = int(record[4])
            deceased = int(record[5])

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=province,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'South Korea',
                'countrycode': 'KOR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid,
                'confirmed': confirmed,
                'dead': deceased,
                'recovered': released
            }
            self.db.upsert_epidemiology_data(**upsert_obj)
