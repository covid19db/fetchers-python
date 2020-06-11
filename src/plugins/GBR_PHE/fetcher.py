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

import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from io import StringIO
import requests

__all__ = ('EnglandFetcher',)

logger = logging.getLogger(__name__)


class EnglandFetcher(AbstractFetcher):
    ''' a fetcher to collect data for English lower tier local authorities'''
    LOAD_PLUGIN = True
    SOURCE = 'GBR_PHE'  # Public Health England

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://coronavirus.data.gov.uk/downloads/csv/coronavirus-cases_latest.csv'
        r = requests.get(url)
        return pd.read_csv(StringIO(r.text))

    def run(self):
        data = self.fetch()

        for index, record in data.iterrows():
            if record[2] != 'Lower tier local authority':
                continue

            date = str(record[3])
            lau = record[0]
            confirmed = int(record[7])

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=lau,
                input_adm_area_2=None,
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
                'adm_area_3': adm_area_3,
                'gid': gid,
                'confirmed': confirmed,
            }

            self.db.upsert_epidemiology_data(**upsert_obj)
