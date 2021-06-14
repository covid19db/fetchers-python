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
# Covid-19 Infected Situation Reports
# https://covid19.th-stat.com/en/api
#

import logging
import requests
from datetime import datetime

__all__ = ('ThailandSTATFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class ThailandSTATFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'THA_STAT'

    def fetch(self, category):
        return requests.get(f'https://covid19.th-stat.com/json/covid19v2/get{category}.json').json()

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch('Timeline')

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
            self.upsert_data(**upsert_obj)

        logger.debug('Fetching regional information')
        data = self.fetch('SumCases')
        lastdata = data['LastData'][:10]

        for record in data['Province']:
            provinceen = record['ProvinceEn']
            if provinceen == 'Unknown':
                continue

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=provinceen,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': lastdata,
                'country': 'Thailand',
                'countrycode': 'THA',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid,
                'confirmed': int(record['Count'])
            }
            self.upsert_data(**upsert_obj)
