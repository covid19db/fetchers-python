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
import requests
from datetime import date

__all__ = ('NigeriaSO',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class NigeriaSO(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'NGA_SO'

    def fetch(self):
        url = 'https://covidnigeria.herokuapp.com/'
        data = requests.get(url).json()
        return data["data"]

    def run(self):
        logger.info("Processing total number of cases in Nigeria")

        data = self.fetch()

        date_ = date.today().strftime('%Y-%m-%d')
        tested = int(data["totalSamplesTested"])
        confirmed = int(data["totalConfirmedCases"])
        recovered = int(data["discharged"])
        dead = int(data["death"])

        # we need to build an object containing the data we want to add or update
        upsert_obj = {
            # source is https://github.com/sink-opuba/covid-19-nigeria-api
            # Pulls information from Nigeria Centre for Disease Control, https://covid19.ncdc.gov.ng/
            'source': self.SOURCE,
            'date': date_,
            'country': 'Nigeria',
            'countrycode': 'NGA',
            'adm_area_1': None,
            'adm_area_2': None,
            'adm_area_3': None,
            'tested': tested,
            'confirmed': confirmed,
            'dead': dead,
            'recovered': recovered,
            'gid': ['NGA']
        }

        self.upsert_data(**upsert_obj)
