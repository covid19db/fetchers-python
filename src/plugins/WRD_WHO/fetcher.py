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
from io import StringIO
import requests

__all__ = ('WorldWHOFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class WorldWHOFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data for world data from World Health Organization (WHO)'''
    LOAD_PLUGIN = True
    SOURCE = 'WRD_WHO'  # World Health Organization (WHO)

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
        r = requests.get(url)
        return pd.read_csv(StringIO(r.text))

    def run(self):
        data = self.fetch()

        for index, record in data.iterrows():
   
            date = str(record[0])
            country_code = str(record[1])
            country = str(record[2])
            confirmed = int(record[5])
            dead = int(record[7])

          
            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': country,
                'countrycode': country_code,
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': [country_code],
                'confirmed': confirmed,
                'dead': dead,
            }

            self.upsert_data(**upsert_obj)
