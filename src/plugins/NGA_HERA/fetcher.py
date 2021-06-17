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
import math
from datetime import datetime

__all__ = ('NigeriaHERA',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class NigeriaHERA(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'NGA_HERA'

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://data.humdata.org/dataset/f5c35452-d766-468a-a272-4bd82d0a3be0/resource/e8777e62-870d-41a7-952f-97c6ff977706/download/nga_subnational_covid19_hera.csv'
        return pd.read_csv(url,sep=';')

    def run(self):
        data = self.fetch()

        for index, record in data.iterrows():
            olddate = str(record[1])  # date is in dd/mm/yyyy format
            state = record[5]
            confirmed = None if math.isnan(record[6]) else int(record[6])
            dead = None if math.isnan(record[7]) else int(record[7])
            recovered = None if math.isnan(record[8]) else int(record[8])

            # convert the date format to be in YYYY-MM-DD format as expected
            datetimeobject = datetime.strptime(olddate, '%d/%m/%Y')
            date = datetimeobject.strftime('%Y-%m-%d')

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True,
                suppress_exception=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'Nigeria',
                'countrycode': 'NGA',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered
            }

            self.upsert_data(**upsert_obj)
