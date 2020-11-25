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

import re
import logging
import requests
import pandas as pd
from io import StringIO
from pandas import DataFrame

from utils.config import config
from .mapping import RegionMapping
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

__all__ = ('PolandGovFetcher',)

logger = logging.getLogger(__name__)


class PolandGovFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'POL_GOV'

    def update_cases(self, date: str, df: DataFrame, data_type: str, region_mapping: RegionMapping):
        for index, row in df.iterrows():
            if not row['Powiat/Miasto']:
                continue

            adm_area_1, adm_area_2, adm_area_3, gid = region_mapping.find_nearest_translation(
                region_name=row['Powiat/Miasto'], adm_area_1=row['Województwo'])

            print(row['Województwo'], row['Powiat/Miasto'], adm_area_1, adm_area_2, adm_area_3, gid)
            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'Poland',
                'countrycode': 'POL',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid
            }

            if data_type == 'confirmed':
                upsert_obj['confirmed'] = row[df.columns[2]]
            elif data_type == 'deaths':
                upsert_obj['dead'] = row[df.columns[2]]
            else:
                raise Exception('Data type not supported!')

            self.upsert_data(**upsert_obj)

    def run(self):
        url = "https://www.gov.pl/attachment/94468b53-7215-40e7-bdd2-a6e18f90f00c"

        req = requests.get(url)
        req.encoding = 'cp1250'
        day, month, year = re.findall(r"filename\*?=.+'(\d+)_(\d+)_(\d+)_.+csv", req.headers['content-disposition'])[0]
        date = f"{year}-{month}-{day}"
        df_data = pd.read_csv(StringIO(req.text), sep=';', decimal=",")

        region_mapping = RegionMapping(self.data_adapter.conn)

        self.update_cases(date,
                          df_data[['Województwo', 'Powiat/Miasto', 'Liczba']],
                          'confirmed',
                          region_mapping)

        self.update_cases(date,
                          df_data[['Województwo', 'Powiat/Miasto', 'Wszystkie przypadki śmiertelne']],
                          'deaths',
                          region_mapping)
