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

import logging
import pandas as pd
import requests
from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime

__all__ = ('IndonesiaFetcher',)

logger = logging.getLogger(__name__)


class IndonesiaFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'IDN_GTPPC'

    def fetch(self, url):
        data = requests.get(url).json()
        df = pd.DataFrame(data["list_perkembangan"])
        return df

    def update_national_cases(self):
        logger.info("Processing total number of cases in Indonesia")

        url = 'https://data.covid19.go.id/public/api/update.json'
        data = requests.get(url).json()

        # Extract relevant portion of json file
        df = pd.DataFrame(data["update"]["harian"])

        for index, record in df.iterrows():
            # date is in old json format
            d = str(record[1])

            # convert the date format to be in YYYY-MM-DD format as expected
            d = int(d[:10])
            date = datetime.fromtimestamp(d).strftime('%Y-%m-%d')

            dead = int(record[9].get('value'))  # jumlah_meninggal_kum
            confirmed = int(record[7].get('value'))  # jumlah_positif_kum
            recovered = int(record[8].get('value'))  # jumlah_sembuh_kum

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                # source is Gugus Tugas Percepatan Penanganan COVID-19
                'source': self.SOURCE,
                'date': date,
                'country': 'Indonesia',
                'countrycode': 'IDN',
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,
                'gid': ['IDN']
            }

            self.db.upsert_epidemiology_data(**upsert_obj)

    def update_provincial_cases(self):
        logger.info("Processing number of cases in Indonesia by province")

        # each province has data in a different json file
        # call each province by its entry in translation.csv
        for province in self.adm_translator.translation_pd['input_adm_area_1']:
            # the csv contains a header row which must be skipped
            if province == 'input_adm_area_1':
                continue

            url = 'https://data.covid19.go.id/public/api/prov_detail_' + province + '.json'
            data = self.fetch(url)

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=province,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            for index, record in data.iterrows():
                # date is in old json format
                d = str(record[0])

                # convert the date format to be in YYYY-MM-DD format as expected
                d = int(d[:10])
                date = datetime.fromtimestamp(d).strftime('%Y-%m-%d')

                dead = int(record[7])
                confirmed = int(record[5])
                recovered = int(record[6])

                # we need to build an object containing the data we want to add or update
                upsert_obj = {
                    # source is Gugus Tugas Percepatan Penanganan COVID-19
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'Indonesia',
                    'countrycode': 'IDN',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'confirmed': confirmed,
                    'dead': dead,
                    'recovered': recovered,
                    'gid': gid
                }

                self.db.upsert_epidemiology_data(**upsert_obj)

    def run(self):
        self.update_national_cases()
        self.update_provincial_cases()
