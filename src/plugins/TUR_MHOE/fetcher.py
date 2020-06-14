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

from datetime import datetime
import logging
import pandas as pd
import numpy as np

__all__ = ('TUR_MHOE',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

""" 
    site-location: https://github.com/ozanerturk/covid19-turkey-api
    
    COVID19-Turkey Data for Turkey created, maintained and hosted by ozanerturk.
    
    The data sources include: Turkish Ministry of Health (pulling every 5 mins).
    
"""
logger = logging.getLogger(__name__)


class TUR_MHOE(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'TUR_MHOE'

    def country_fetch(self):
        """
                        This url mainly provide cumulative data on the country-level.
        """

        url = 'https://raw.githubusercontent.com/ozanerturk/covid19-turkey-api/master/dataset/timeline.csv'
        logger.debug('Fetching Turkey country-level data from TUR_MHOE')
        return pd.read_csv(url)

    def run(self):
        """
                        This run function mainly created country-level cumulative tested&confirmed&dead&ICU&recovered
                        collection from country_fetch;
        
        """

        country_data = self.country_fetch()

        ### Get dates & tested & confirmed & dead & hospitalised_icu & recovered lists
        time_list = list(country_data.date)
        tested_list = np.asarray(country_data.totalTests)
        confirmed_list = np.asarray(country_data.totalCases)
        dead_list = np.asarray(country_data.totalDeaths)
        HOSPITALISED_ICU_list = np.asarray(country_data.totalIntensiveCare)
        recovered_list = np.asarray(country_data.totalRecovered)

        for k in range(len(time_list)):
            ### Translating data format from DD/MM/YYYY to YYYY-MM-DD
            date_ddmmyy = time_list[k]
            date = datetime.strptime(date_ddmmyy, '%d/%m/%Y').strftime('%Y-%m-%d')

            tested = tested_list[k]
            confirmed = confirmed_list[k]
            dead = dead_list[k]
            hospitalised_icu = HOSPITALISED_ICU_list[k]
            recovered = recovered_list[k]

            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                'source': self.SOURCE,
                # date is also mandatory, the format must be YYYY-MM-DD
                'date': date,
                # country is mandatory and should be in English
                # the exception is "Ships"
                'country': "Turkey",
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'TUR',
                # adm_area_1, when available, is a wide-area administrative region, like a
                # Canadian province in this case. There are also subareas adm_area_2 and
                # adm_area_3
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': ['TUR'],
                'tested': int(tested),
                'confirmed': int(confirmed),
                # dead is the number of people who have died because of covid19, this is cumulative
                'dead': int(dead),
                'hospitalised_icu': int(hospitalised_icu),
                'recovered': int(recovered)

            }

            self.upsert_data(**upsert_obj)
