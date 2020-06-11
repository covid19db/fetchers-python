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
import json
import requests
from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime
from bs4 import BeautifulSoup

__all__ = ('RussiaGovFetcher',)

logger = logging.getLogger(__name__)


class RussiaGovFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'RUS_GOV'

    def fetch_regional(self, url):
        data = requests.get(url).json()
        df = pd.DataFrame(data)
        return df

    def fetch_national(self):
        # the data is contained in the cv-stats-virus tag of this webpage
        url = 'https://xn--80aesfpebagmfblc0a.xn--p1ai/information/'
        website_content = requests.get(url)
        soup = BeautifulSoup(website_content.text, 'lxml')
        stats = soup.find("cv-stats-virus")

        # one attribute of the cv-stats-virus tag is :charts-data
        # this is a json string - read it with json.loads and then load to pandas
        data = json.loads(stats[':charts-data'])
        df = pd.DataFrame(data)
        return df

    def update_national_cases(self):
        logger.info("Processing total number of cases in Russia")
        df = self.fetch_national()

        for index, record in df.iterrows():
            # convert the date format to be in YYYY-MM-DD format as expected
            d = str(record['date'])
            date = datetime.strptime(d, '%d.%m.%Y').strftime('%Y-%m-%d')

            dead = int(record['died'])
            confirmed = int(record['sick'])
            recovered = int(record['healed'])

            # build an object containing the data we want to add or update
            upsert_obj = {
                # source is official government website
                # https://xn--80aesfpebagmfblc0a.xn--p1ai/information/ (stopcoronavirus.rf)
                'source': self.SOURCE,
                'date': date,
                'country': 'Russia',
                'countrycode': 'RUS',
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,
                'gid': ['RUS']
            }

            self.db.upsert_epidemiology_data(**upsert_obj)

    def update_provincial_cases(self):
        logger.info("Processing number of cases in Russia by province")

        # each province has data in a different json file
        # call each province by its entry in translation.csv
        # this is the short code used by the website
        # John has a Jupyter notebook RussianNameCodes which matches codes to provinces - needs manual correction
        for province in self.adm_translator.translation_pd['input_adm_area_1']:
            # the csv contains a header row which must be skipped
            if province == 'input_adm_area_1':
                continue

            url = 'https://xn--80aesfpebagmfblc0a.xn--p1ai/covid_data.json?do=region_stats&code=' + province
            data = self.fetch_regional(url)

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=province,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            for index, record in data.iterrows():
                # convert the date format to be in YYYY-MM-DD format as expected
                d = str(record['date'])
                date = datetime.strptime(d, '%d.%m.%Y').strftime('%Y-%m-%d')

                dead = int(record['died'])
                confirmed = int(record['sick'])
                recovered = int(record['healed'])

                # build an object containing the data we want to add or update
                upsert_obj = {
                    # source is official government website
                    # https://xn--80aesfpebagmfblc0a.xn--p1ai/information/ (stopcoronavirus.rf)
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'Russia',
                    'countrycode': 'RUS',
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
