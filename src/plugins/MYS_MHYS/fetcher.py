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
from datetime import datetime

__all__ = ('MYS_MHYS',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

""" 
    site-location: https://github.com/ynshung/covid-19-malaysia
    
    COVID19-Malaysia Data for Malaysia created, maintained and hosted by ynshung
    
    Data are retrieved from multiple offical sources such as:

        MOH Facebook Page
        CPRC KKM Telegram Channel
        Desk of DG
        KKM Portal MyHealth Twitter
        Twitter of DG
        Info Sihat.
    
"""
logger = logging.getLogger(__name__)


class MYS_MHYS(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'MYS_MHYS'

    def province_confirmed_fetch(self):

        """
                        This url mainly provide cumulative data of confirmed cases on the province-level.
        """

        url = 'https://raw.githubusercontent.com/ynshung/covid-19-malaysia/master/covid-19-my-states-cases.csv'

        logger.debug('Fetching Malaysia province-level confirmed cases from MYS_MHYS')
        return pd.read_csv(url, na_values='-')

    def country_fetch(self):

        """
                        This url mainly provide cumulative data of confirmed/dead/icu cases on the country-level.
        """

        url = 'https://raw.githubusercontent.com/ynshung/covid-19-malaysia/master/covid-19-malaysia.csv'
        logger.debug('Fetching Malaysia country-level confirmed/dead/icu cases from MYS_MHYS')

        return pd.read_csv(url)

    def run(self):

        """
                        This run function mainly created country-level cumulative confirmed/dead/icu collection from
                        
                        country_fetch and province_confirmed_fetch;
        
        """

        country_data = self.country_fetch()

        date_list = list(country_data.date)
        confirmed_list = np.asarray(country_data.cases, dtype='int')
        dead_list = np.asarray(country_data.death, dtype='int')
        icu_list = np.asarray(country_data.icu, dtype='int')

        for j in range(len(date_list)):

            date = datetime.strptime(date_list[j], '%d/%m/%Y').strftime('%Y-%m-%d')
            confirmed = int(confirmed_list[j])
            dead = int(dead_list[j])
            icu = int(icu_list[j])

            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                'source': self.SOURCE,
                # date is also mandatory, the format must be YYYY-MM-DD
                'date': date,
                # country is mandatory and should be in English
                # the exception is "Ships"
                'country': 'Malaysia',
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'MYS',
                # adm_area_1, when available, is a wide-area administrative region, like a
                # Canadian province in this case. There are also subareas adm_area_2 and
                # adm_area_3
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': ['MYS'],
                'confirmed': int(confirmed),
                # dead is the number of people who have died because of covid19, this is cumulative
                'dead': int(dead),
                'hospitalised_icu': int(icu)

            }
            self.upsert_data(**upsert_obj)

        province_confirmed_data = self.province_confirmed_fetch()
        province_list = list(province_confirmed_data.columns[1:])
        provincedata_list = list(province_confirmed_data.date)

        for ii in range(len(province_list)):

            province_confirmed_list = np.asarray(province_confirmed_data[province_list[ii]])

            for k in range(len(provincedata_list)):

                date = datetime.strptime(provincedata_list[k], '%d/%m/%Y').strftime('%Y-%m-%d')

                province_confirmed = province_confirmed_list[k]

                if pd.isna(province_confirmed):
                    province_confirmed = None
                else:
                    province_confirmed = int(province_confirmed)

                success, adm_area_1, adm_area_2, adm_area_3, gid \
                    = self.adm_translator.tr(input_adm_area_1=province_list[ii],
                                             input_adm_area_2=None,
                                             input_adm_area_3=None,
                                             return_original_if_failure=True)

                upsert_obj = {  # source is mandatory and is a code that identifies the  source
                    'source': self.SOURCE,
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': 'Malaysia',
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'MYS',
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'gid': gid,
                    'confirmed': province_confirmed
                }
                self.upsert_data(**upsert_obj)
