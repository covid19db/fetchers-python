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
import requests
from datetime import datetime
from bs4 import BeautifulSoup

__all__ = ('LebanonGovFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class LebanonGovFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data from Lebanon Ministry of Health'''
    LOAD_PLUGIN = True
    SOURCE = 'LEB_GOV'  # Lebanon Ministry of Health

    def fetch(self):
        url = 'https://corona.ministryinfo.gov.lb/'
        website_content = requests.get(url)
        soup = BeautifulSoup(website_content.text, 'lxml')
        lst = []
        divs = soup.find_all('div', class_='counter-content')
        for elem in divs:
            h = elem.find('h1').get_text()
            lst.append(h.strip())           
        df = pd.DataFrame(lst)
    
        return df
        
        
    def run(self):
        logger.info("Processing total number of cases in Lebanon")
        data = self.fetch()
        
        date = datetime.today().strftime('%Y-%m-%d')
        dead = int(data.iloc[ 7 , : ])
        confirmed = int(data.iloc[ 1 , : ])
        recovered = int(data.iloc[ 8 , : ])
        tested = int(data.iloc[ 9 , : ])
        hospitalised_icu = int(data.iloc[ 6 , : ])
        quarantined = int(data.iloc[ 15 , : ])
             
        upsert_obj = {
            'source': self.SOURCE,
            'date': date,
            'country': 'Lebanon',
            'countrycode': 'LEB',
            'adm_area_1': None,
            'adm_area_2': None,
            'adm_area_3': None,
            'gid': None,
            'confirmed': confirmed,
            'recovered': recovered,
            'dead': dead,
            'tested' : tested,
            'hospitalised_icu' : hospitalised_icu,
            'quarantined' : quarantined,
        }

        self.upsert_data(**upsert_obj)
