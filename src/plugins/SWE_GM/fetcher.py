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
from datetime import date

__all__ = ('SWE_GMFFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

""" 
    
    COVID19-Sweden Data for Sweden created, maintained and hosted by elinlutz
    
    Mapping the coronavirus in react, gatsby.js and leaflet https://coronakartan.se.
    
"""
logger = logging.getLogger(__name__)


class SWE_GMFFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'SWE_GM'

    def province_confirmed_fetch(self):

        """
                        This url mainly provide non-cumulative data of confirmed cases on the province-level.
        """

        url = 'https://raw.githubusercontent.com/elinlutz/gatsby-map/master/src/data/time_series/time_series_confimed-confirmed.csv'
        logger.debug('Fetching Sweden province-level confirmed cases from SWE_GM')
        return pd.read_csv(url)

    def province_dead_fetch(self):

        """
                        This url mainly provide non-cumulative data of confirmed cases on the province-level.
        """

        url = 'https://raw.githubusercontent.com/elinlutz/gatsby-map/master/src/data/time_series/time_series_deaths-deaths.csv'
        logger.debug('Fetching Sweden province-level death cases from SWE_GM')
        return pd.read_csv(url)

    def fix_naming(self, province):
        # Fix naming
        if province == 'Kalmar län':
            province = 'Kalmar'
        elif province == 'Sörmland':
            province = 'Södermanland'
        elif province == 'Örebro':
            province = 'Orebro'
        elif province == 'Okänt':
            province = 'UNKNOWN'
        return province

    def run(self):

        """
                        This run function mainly created province-level cumulative confirmed&dead collection from
                        
                        province_confirmed_fetch and province_dead_fetch;
        
        """

        province_confirmed_data = self.province_confirmed_fetch()
        province_dead_data = self.province_dead_fetch()

        ### Get province names list
        province_list = list(province_confirmed_data["Display_Name"])[:-2]

        ### Get dates list

        date_list = list(province_confirmed_data.columns[5:-11])

        ### Get province data day by day
        for j in range(len(province_list)):

            # current province
            province = self.fix_naming(province_list[j])

            if province != 'UNKNOWN':
                adm_area_1, adm_area_2, adm_area_3, gid = self.get_region('SWE', province)
                if not gid:
                    raise Exception(f'Unable to obtain GID for: {province}')
            else:
                adm_area_1, adm_area_2, adm_area_3, gid = province, None, None, None

            # data is not cumulative, so need to cumulate them by adding previous cumulative data
            previous_confirmed = 0
            previous_dead = 0

            # for current province, go through data from the beginning data day to today
            for i in range(len(date_list)):

                # 'current' date
                date_ = date_list[i]

                # current confirmed for current' date
                current_confirmed = np.array(province_confirmed_data[date_])[j]

                # if current confirmed is missing, replace it by 0, as we need to cumulate the data
                if pd.isnull(current_confirmed):
                    current_confirmed = 0

                # cumulative data by adding confirmed case of the date to the cumulative confirmed cases before this date
                confirmed = int(previous_confirmed + current_confirmed)

                # current dead cases for current' date
                current_dead = np.array(province_dead_data[date_])[j]

                # if current dead is missing, replace it by 0, as we need to cumulate the data
                if pd.isnull(current_dead):
                    current_dead = 0
                # cumulative data by adding dead case of the date to the cumulative dead cases before this date
                dead = int(previous_dead + current_dead)

                if date_ == 'Today':
                    date_ = date.today().strftime("%Y-%m-%d")

                upsert_obj = {
                    # source is mandatory and is a code that identifies the  source
                    'source': self.SOURCE,
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date_,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': 'Sweden',
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'SWE',
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    'adm_area_1': province,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'gid': gid,
                    'confirmed': confirmed,
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': dead

                }
                self.upsert_data(**upsert_obj)

                # replace previous cumulative data by the current cumulative date for iteration
                previous_confirmed = confirmed
                previous_dead = dead

            ###The original csv does not use date format for today's date (which is the last date to collect), but use 'Today', so we should find the current date format first
            today = date.today()
            date_ = today.strftime("%Y-%m-%d")

            ### all the following procedure is exactly the same as before, but only for Today's data
            current_confirmed = np.array(province_confirmed_data["Today"])[j]

            if pd.isnull(current_confirmed):
                current_confirmed = 0
            confirmed = int(previous_confirmed + current_confirmed)

            current_dead = np.array(province_dead_data["Today"])[j]

            if pd.isnull(current_dead):
                current_dead = 0
            dead = int(previous_dead + current_dead)

            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                'source': self.SOURCE,
                # date is also mandatory, the format must be YYYY-MM-DD
                'date': date_,
                # country is mandatory and should be in English
                # the exception is "Ships"
                'country': 'Sweden',
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'SWE',
                # adm_area_1, when available, is a wide-area administrative region, like a
                # Canadian province in this case. There are also subareas adm_area_2 and
                # adm_area_3
                'adm_area_1': adm_area_1,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': gid,
                'confirmed': confirmed,
                # dead is the number of people who have died because of covid19, this is cumulative
                'dead': dead

            }
            self.upsert_data(**upsert_obj)
