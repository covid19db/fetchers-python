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
# https://covid19.ddc.moph.go.th/
#

import logging
import requests
from datetime import datetime

__all__ = ('ThailandMOPHFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class ThailandMOPHFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'THA_MOPH'

    def fetch(self, category):
        return requests.get(f'https://covid19.ddc.moph.go.th/api/Cases/{category}').json()

    def run(self):
        logger.debug('Fetching country-level information')
        old_data = self.fetch('round-1to2-all')
        new_data = self.fetch('timeline-cases-all')
        for dataset in [old_data, new_data]:
            for record in dataset:
                upsert_obj = {
                    'source': self.SOURCE,
                    'date': record['txn_date'],
                    'country': 'Thailand',
                    'countrycode': 'THA',
                    'gid': ['THA'],
                    'confirmed': int(record['total_case']),
                    'dead': int(record['total_death']),
                    'recovered': int(record['total_recovered'])
                }
                self.upsert_data(**upsert_obj)


        logger.debug('Fetching regional information')
        old_data = self.fetch('round-1to2-by-provinces')
        new_data = self.fetch('timeline-cases-by-provinces')

        for dataset in [old_data, new_data]:
            for record in dataset:
                province = record['province']

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=province,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True,
                    suppress_exception=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': record['txn_date'],
                    'country': 'Thailand',
                    'countrycode': 'THA',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'adm_area_3': adm_area_3,
                    'gid': gid,
                    'confirmed': int(record['total_case']),
                    'dead': int(record['total_death'])
                }
                self.upsert_data(**upsert_obj)
