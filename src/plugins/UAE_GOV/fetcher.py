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

__all__ = ('UAEGovFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class UAEGovFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data from United Arab Emirates Government (UAE)'''
    LOAD_PLUGIN = True
    SOURCE = 'UAE_GOV'  # United Arab Emirates Government 

    def fetch(self):
        url = 'https://covid19.ncema.gov.ae/en'
        website_content = requests.get(url,verify=False)
        soup = BeautifulSoup(website_content.text, 'lxml')
        lst = []
        divs = soup.find_all('div', class_='main-content')
        for ul in divs:
            elem = ul.find_all('ul', class_='top-statistics block-separator')
            for  data in elem:
                elem1 = data.find_all('li')
                for  li in elem1:
                    dt = li.find('span').get_text()
                    lst.append(dt.strip())
          
        df = pd.DataFrame(lst)
        return df
        
        
    def run(self):
        logger.info("Processing total number of cases in United Arab Emirates")
        data = self.fetch()
        
        date = datetime.today().strftime('%Y-%m-%d')
        confirmed = int(data.iloc[ 0 , : ])
        dead = int(data.iloc[ 2 , : ])
        recovered = int(data.iloc[ 3 , : ])
                
        upsert_obj = {
            'source': self.SOURCE,
            'date': date,
            'country': 'United Arab Emirates',
            'countrycode': 'UAE',
            'adm_area_1': None,
            'adm_area_2': None,
            'adm_area_3': None,
            'gid': None,
            'confirmed': confirmed,
            'recovered': recovered,
            'dead': dead,
        }

        self.upsert_data(**upsert_obj)


