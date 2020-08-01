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

# Pulling directly from Nigeria Centre for Disease Control, https://covid19.ncdc.gov.ng/

import logging
import requests
from datetime import date
from bs4 import BeautifulSoup

__all__ = ('NigeriaCDC',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class NigeriaCDC(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'NGA_CDC'

    def fetch(self):
        url = 'https://services5.arcgis.com/Y2O5QPjedp8vHACU/arcgis/rest/services/NgeriaCovid19/FeatureServer/0/query' \
              '?f=json&where=ConfCases%20%3E%3D%200&returnGeometry=false&returnSpatialRel=false&outFields=*'
        data = requests.get(url).json()
        return data["features"]

    def fetch_national(self):
        def get_value(tag):
            try:
                result = tag.find_next_sibling("h2").text
                result = result.replace('\n', '').replace(',', '')
                return int(result)
            except:
                return None

        url = 'https://covid19.ncdc.gov.ng/'
        website_content = requests.get(url, verify=False)
        soup = BeautifulSoup(website_content.text, 'lxml')
        cardTitles = soup.find_all('h6')

        titles = [card.text for card in cardTitles]
        values = [get_value(card) for card in cardTitles]
        data = dict(zip(titles, values))
        return data

    def run(self):

        date_ = date.today().strftime('%Y-%m-%d')

        logger.info("Processing number of cases in Nigeria nationally")
        data = self.fetch_national()

        upsert_obj = {
            # Pulling directly from Nigeria Centre for Disease Control, https://covid19.ncdc.gov.ng/
            'source': self.SOURCE,
            'date': date_,
            'country': 'Nigeria',
            'countrycode': 'NGA',
            'adm_area_1': None,
            'adm_area_2': None,
            'adm_area_3': None,
            'tested': data['Samples Tested'],
            'confirmed': data['Confirmed Cases'],
            'dead': data['Death'],
            'recovered': data['Discharged Cases'],
            'gid': ['NGA']
        }

        self.upsert_data(**upsert_obj)

        logger.info("Processing number of cases in Nigeria by province")
        data = self.fetch()

        for record in data:
            details = record.get('attributes')
            state = details.get('NAME_1')
            confirmed = int(details.get('ConfCases'))
            recovered = int(details.get('Recovery'))
            dead = int(details.get('Deaths'))

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date_,
                'country': 'Nigeria',
                'countrycode': 'NGA',
                'adm_area_1': adm_area_1,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,
                'gid': gid
            }

            self.upsert_data(**upsert_obj)
