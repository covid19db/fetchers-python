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
# Data from Santé Publique France
# https://www.data.gouv.fr/fr/datasets/donnees-hospitalieres-relatives-a-lepidemie-de-covid-19/
#

import logging
import pandas as pd

__all__ = ('FranceSPFFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class FranceSPFFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'FRA_SPF'

    def fetch(self, stable):
        url = f'https://www.data.gouv.fr/fr/datasets/r/{stable}'
        return pd.read_csv(url, sep=';', dtype={'dep': str, 'reg': str})

    def run(self):
        logger.debug('Fetching departmental information')
        data = self.fetch('63352e38-d353-4b54-bfd1-f1b3ee1cabd7')
        data = data[data['sexe'] == 0]

        for index, record in data.iterrows():
            # dep;sexe;jour;hosp;rea;rad;dc
            if pd.notna(record[0]):
                dep = 'Département ' + record[0]
                jour = record[2]
                hosp = int(record[3])
                rea = int(record[4])
                rad = int(record[5])
                dc = int(record[6])

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    country_code='FRA',
                    input_adm_area_1=None,
                    input_adm_area_2=dep,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': jour,
                    'country': 'France',
                    'countrycode': 'FRA',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'adm_area_3': adm_area_3,
                    'recovered': rad,
                    'dead': dc,
                    'hospitalised': hosp,
                    'hospitalised_icu': rea,
                    'gid': gid
                }
                self.upsert_data(**upsert_obj)

        logger.debug('Fetching regional information')
        data = self.fetch('08c18e08-6780-452d-9b8c-ae244ad529b3')
        data = data[data['cl_age90'] == 0]

        for index, record in data.iterrows():
            # reg;cl_age90;jour;hosp;rea;rad;dc
            reg = 'Région ' + record[0]
            jour = record[2]
            hosp = int(record[3])
            rea = int(record[4])
            rad = int(record[5])
            dc = int(record[6])

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='FRA',
                input_adm_area_1=reg,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': jour,
                'country': 'France',
                'countrycode': 'FRA',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'recovered': rad,
                'dead': dc,
                'hospitalised': hosp,
                'hospitalised_icu': rea,
                'gid': gid
            }
            self.upsert_data(**upsert_obj)
