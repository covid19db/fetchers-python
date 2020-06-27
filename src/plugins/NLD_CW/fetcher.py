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
import numpy as np

__all__ = ('NLD_CWFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

""" 
    site-location: https://github.com/J535D165/CoronaWatchNL
    
    COVID19-Netherlands Data for Netherlands created, maintained and hosted by CoronaWatchNL.
    
    The data sources include: National Institute for Public Health and the Environment 
        
    
"""
logger = logging.getLogger(__name__)


class NLD_CWFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'NLD_CW'

    def country_fetch(self):

        """
                        This url mainly provide cumulative confirmed&dead&hospitalised data on the country-level.
        """

        url = 'https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data/rivm_NL_covid19_national.csv'

        logger.debug('Fetching Netherland country-level confirmed-dead-hospitalised data from NLD_CW')

        return pd.read_csv(url)
    
    
    
    def province_fetch(self):

        """
                        This url mainly provide cumulative confirmed data on the province-level.
        """

        url = 'https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data/rivm_NL_covid19_province.csv'

        logger.debug('Fetching Netherland province-level confirmed data from NLD_CW')

        return pd.read_csv(url)        
        

    def run(self):

        """
                        This run function mainly created country-level cumulative confirmed-dead-hospitalised
                        collection from country_fetch;
        
        """

        country_data = self.country_fetch()
        province_data=self.province_fetch()

        ### Get dates & cumulative recorded lists
        time_list = list(country_data.Datum)
        lists = np.asarray(country_data.Aantal, dtype='int')

        for k in range(len(time_list)):

            ### Translating data format from DD/MM/YYYY to YYYY-MM-DD
            if k % 3 == 0:
                confirmed = lists[k]
                if confirmed < 0:
                    confirmed = None
                else:
                    confirmed = int(confirmed)
            elif k % 3 == 1:
                HOSPITALISED = lists[k]
                if HOSPITALISED < 0:
                    HOSPITALISED = None
                else:
                    HOSPITALISED = int(HOSPITALISED)
            else:
                date = time_list[k]
                dead = lists[k]
                if dead < 0:
                    dead = None
                else:
                    dead = int(dead)

                upsert_obj = {
                    # source is mandatory and is a code that identifies the  source
                    'source': self.SOURCE,
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': "Netherlands",
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'NLD',
                    'gid': ['NLD'],
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    'adm_area_1': None,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'confirmed': confirmed,
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': dead,
                    'hospitalised': HOSPITALISED
                }

                self.upsert_data(**upsert_obj)
                
                
        valid_provinces=list(set(province_data.Provincienaam))
        
        for province in valid_provinces[1:]:
            
                
                current_data=province_data[province_data.Provincienaam==province]
            
                date_list=list(province_data[province_data.Provincienaam==province].Datum)
            
                confirmed_list=np.array(province_data[province_data.Provincienaam==province].Aantal,dtype='int')
            
            
                for i in range(len(date_list)):
                
                    date=date_list[i]
                    confirmed=confirmed_list[i]
                
                    adm_area_1, adm_area_2, adm_area_3, gid = province, None, None, None
                
                    upsert_obj = {
                        # source is mandatory and is a code that identifies the  source
                        'source': self.SOURCE,
                        # date is also mandatory, the format must be YYYY-MM-DD
                        'date': date,
                        # country is mandatory and should be in English
                        # the exception is "Ships"
                        'country': "Netherlands",
                        # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                        # an exception is ships, which has "---" as country code
                        'countrycode': 'NLD',
                        'gid': ['NLD'],
                        # adm_area_1, when available, is a wide-area administrative region, like a
                        # Canadian province in this case. There are also subareas adm_area_2 and
                        # adm_area_3
                        'adm_area_1': adm_area_1,
                        'adm_area_2': None,
                        'adm_area_3': None,
                        'confirmed': int(confirmed)
                        }

                    self.upsert_data(**upsert_obj)

               
                
            
            
            
            

