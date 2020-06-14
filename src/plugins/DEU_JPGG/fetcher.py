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
# Germany, data collected by Jan-Philip Gehrcke, from the
# Public Health Offices (Gesundheitsaemter) in Germany
# https://gehrcke.de/2020/03/covid-19-sars-cov-2-resources/
# github: https://github.com/jgehrcke/covid-19-germany-gae
#

import logging
import requests

__all__ = ('GermanyJPGGFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class GermanyJPGGFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'DEU_JPGG'

    def fetch(self, state):
        logger.debug(f'Fetching cases for Germany region {state}')
        data = requests.get(f'https://covid19-germany.appspot.com/timeseries/{state}/cases').json()

        for record in data['data']:
            date = next(iter(record))

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='DEU',
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date[:10],
                'country': 'Germany',
                'countrycode': 'DEU',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'confirmed': int(record[date]),
                'gid': gid
            }
            self.upsert_data(**upsert_obj)

        logger.debug(f'Fetching deaths for Germany region {state}')
        data = requests.get(f'https://covid19-germany.appspot.com/timeseries/{state}/deaths').json()

        for record in data['data']:
            date = next(iter(record))

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='DEU',
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date[:10],
                'country': 'Germany',
                'countrycode': 'DEU',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'dead': int(record[date]),
                'gid': gid
            }
            self.upsert_data(**upsert_obj)

    def run(self):
        for state in self.adm_translator.translation_pd['input_adm_area_1']:
            self.fetch(state)
