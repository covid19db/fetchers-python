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

__all__ = ('ZAF_DSFSIFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

""" 
    site-location: https://github.com/dsfsi/covid19za
    
    COVID19_ZA Data for South Africa created, maintained and hosted by Data Science for Social Impact research group,
    led by Dr. Vukosi Marivate, at the University of Pretoria. 
    
    Their data sources include: NICD - South Africa; Department of Health - South Africa; 
                                South African Government Media Statements;
                                National Department of Health Data Dictionary;
                                MedPages; Statistics South Africa
"""
logger = logging.getLogger(__name__)


class ZAF_DSFSIFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'ZAF_DSFSI'

    def country_fetch(self):

        """
                        This url mainly provide cumulative data of tested, dead,
                                 and hospitalised cases on the country-level.
        """

        url = 'https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_testing.csv'
        logger.debug('Fetching Soutch Africa contry-level test, death and hospitalised cases from ZAF_DSFSI')
        return pd.read_csv(url)

    def province_confirmed_fetch(self):

        """
                        This url mainly provide cumulative data of confirmed cases on the province-level.
        """

        url = 'https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_provincial_cumulative_timeline_confirmed.csv'
        logger.debug('Fetching Soutch Africa province-level confirmed cases from ZAF_DSFSI')
        return pd.read_csv(url)

    def province_dead_fetch(self):

        """
                        This url mainly provide individual cases of death data on the province-level.
        """

        url = 'https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_deaths.csv'
        logger.debug('Fetching Soutch Africa province-level death cases from ZAF_DSFSI')
        return pd.read_csv(url)

    def run(self):

        """
                This run functions handling two things:
                1. created country-level cumulative information collection from country_fetch and province_confirmed_fetch;
                2. created province-level cumulative confirmed&dead collection from province_confirmed_fetch and province_dead_fetch;
        
        """

        country_data = self.country_fetch()
        province_confirmed_data = self.province_confirmed_fetch()
        province_dead_data = self.province_dead_fetch()

        for index, record in country_data.iterrows():

            ### Translating data format from DD-MM-YYYY to YYYY-MM-DD
            date_ddmmyyyy = record[0]
            date = datetime.strptime(date_ddmmyyyy, '%d-%m-%Y').strftime('%Y-%m-%d')

            ### Fetch confirmed case from another csv province_confirmed_data
            #  find the date is included in province_confirmed_data or not:
            #  if yes, fetch the confirmed num upon that date from province_confirmed_data csv;
            #  otherwise, confirmed num is nan
            if date_ddmmyyyy in list(province_confirmed_data['date']):

                temp = province_confirmed_data[province_confirmed_data['date'] == date_ddmmyyyy]

                for index1, record1 in temp.iterrows():
                    confirmed = record1[-2]
            else:
                confirmed = None
            try:
                confirmed = int(confirmed)
            except:
                confirmed = None
            ### Fetch tested, recovered, hopistalised, icu, ventilated and dead nums                    
            cumulative_tests = record[2]
            try:
                cumulative_tests = int(cumulative_tests)
            except:
                cumulative_tests = None

            recovered = record[3]
            try:
                recovered = int(recovered)
            except:
                recovered = None

            hospitalisation = record[4]
            try:
                hospitalisation = int(hospitalisation)
            except:
                hospitalisation = None

            critical_icu = record[5]
            try:
                critical_icu = int(critical_icu)
            except:
                critical_icu = None

            dead = record[7]
            try:
                dead = int(dead)
            except:
                dead = None

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                # source
                'source': self.SOURCE,
                # date
                'date': date,
                # country
                'country': "South Africa",
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'ZAF',
                # adm_area_1, when available, is a wide-area administrative region
                # In this case, adm_area_1, adm_area_2 (subadministrative region) and 
                #  adm_area_3 (subsubadministrative region) are not available.
                'gid': ['ZAF'],
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                # tested number by each date, cumulative
                'tested': cumulative_tests,
                # confirmed is the number of confirmed cases of infection, this is cumulative                    
                'confirmed': confirmed,
                # dead is the number of people who have died because of covid19, this is cumulative
                'dead': dead,
                # recovered is the number of people who have healed, this is cumulative
                'recovered': recovered,
                # hospitalised is the number of people who have been admitted to hospital, this is cumulative                
                'hospitalised': hospitalisation,
                # ICU is the number of people who have been admitted to ICU, this is cumulative                                
                'hospitalised_icu': critical_icu,
                # ventilation is the number of people who have been treated using ventilator equipment, this is cumulative
                # 'hospitalised_ventilation': ventilation

            }

            self.upsert_data(**upsert_obj)

        ### Get the 10 province names    
        province_lists = list(province_confirmed_data.columns)[2:12]

        trans_dict = {'EC': 'Eastern Cape',
                      'FS': 'Free State',
                      'GP': 'Gauteng',
                      'KZN': 'KwaZulu-Natal',
                      'LP': 'Limpopo',
                      'MP': 'Mpumalanga',
                      'NC': 'Northern Cape',
                      'NW': 'North West',
                      'WC': 'Western Cape',
                      'UNKNOWN': 'UNKNOWN'}

        ### Basically using province_confirmed_data csv
        for index_province, record_province in province_confirmed_data.iterrows():

            ### Translating date format from DD-MM-YYYY to YYYY-MM-DD
            date_ddmmyyyy = record_province[0]
            date = datetime.strptime(date_ddmmyyyy, '%d-%m-%Y').strftime('%Y-%m-%d')

            ### Get date numeric format for later use (to get dead number from csv province_dead_fetch)
            date_yyyymmdd = int(record_province[1])

            ### For each date, fetch confirmed and dead numbers from 10 provinces one by one 
            for k in range(len(province_lists)):
                # current province
                province = trans_dict[province_lists[k]]

                # Get confirmed number from current csv (province_confirmed_data) for current province
                confirmed = record_province[k + 2]
                try:
                    confirmed = int(confirmed)
                except:
                    confirmed = None

                # To get cumulative dead number in current province at some date, by counting the individual cases in
                # the same province up to the date ('YYYYMMDD' numeric format) from csv province_dead_data

                dead = len(np.where(np.array(
                    list(province_dead_data[province_dead_data['province'] == province]['YYYYMMDD'])) <= date_yyyymmdd)[
                               0])

                if province != 'UNKNOWN':
                    adm_area_1, adm_area_2, adm_area_3, gid = self.get_region('ZAF', province)

                    if not gid:
                        raise Exception(f'Unable to obtain GID for: {province}')
                else:
                    adm_area_1, adm_area_2, adm_area_3, gid = province, None, None, None

                upsert_obj_province = {
                    # source is mandatory and is a code that identifies the  source
                    'source': self.SOURCE,
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    'country': "South Africa",
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    'countrycode': 'ZAF',
                    # adm_area_1, when available, is a wide-area administrative region
                    # adm_area_2 (subadministrative region) and adm_area_3 (subsubadministrative region) , are not available.
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'gid': gid,
                    # confirmed is the number of confirmed cases of infection, this is cumulative
                    'confirmed': confirmed,
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': int(dead)

                }

                self.upsert_data(**upsert_obj_province)
