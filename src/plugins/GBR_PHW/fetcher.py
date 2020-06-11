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
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('WalesFetcher',)

logger = logging.getLogger(__name__)


class WalesFetcher(AbstractFetcher):
    ''' a fetcher to collect data for testing by Welsh local authorities and deaths by health board'''
    LOAD_PLUGIN = True
    SOURCE = 'GBR_PHW'  # Public Health Wales

    def fetch(self):
        # an excel file to be downloaded
        url = 'http://www2.nphs.wales.nhs.uk:8080/CommunitySurveillanceDocs.nsf/3dc04669c9e1eaa880257062003b246b/77fdb9a33544aee88025855100300cab/$FILE/Rapid%20COVID-19%20surveillance%20data.xlsx'
        testing_data = pd.read_excel(url, sheet_name='Tests by specimen date', parse_dates=[1])
        deaths_data = pd.read_excel(url, sheet_name='Deaths by LHB')

        # collect the date of the last update for deaths from another sheet, as Deaths by LHB does not have a date in it
        national_deaths = pd.read_excel(url, sheet_name='Deaths by date', parse_dates=[0], nrows=1)
        effective_date = national_deaths.at[0, 'Date of death']
        effective_date = effective_date.strftime('%Y-%m-%d')

        return testing_data, deaths_data, effective_date

    def tests(self, data):
        for index, record in data.iterrows():

            date = record[1]
            lau = record[0]
            confirmed = int(record[3])
            tested = int(record[6])

            # skipping cases marked as outside Wales
            if lau == 'Outside Wales':
                continue

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=lau,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True,
                suppress_exception=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date.strftime('%Y-%m-%d'),
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid,
                'confirmed': confirmed,
                'tested': tested
            }

            self.db.upsert_epidemiology_data(**upsert_obj)

            # in order to provide coherent level 3 admin data for users, insert the same records but with a level 3 gid

            if lau == 'Unknown':
                continue

            gid3 = [code.split('_')[0] + '.1_1' for code in gid]

            level3_upsert_obj = {
                'source': self.SOURCE,
                'date': date.strftime('%Y-%m-%d'),
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_2,
                'gid': gid3,
                'confirmed': confirmed,
                'tested': tested
            }

            self.db.upsert_epidemiology_data(**level3_upsert_obj)

    def deaths(self, data, date):
        for index, record in data.iterrows():

            lhb = record[0]
            dead = int(record[1])

            if lhb == 'Resident outside Wales':
                break

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=lhb,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid,
                'dead': dead
            }

            self.db.upsert_epidemiology_data(**upsert_obj)

    def run(self):
        testing_data, deaths_data, effective_date = self.fetch()
        self.tests(testing_data)
        self.deaths(deaths_data, effective_date)
