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
from urllib.error import HTTPError

from datetime import datetime, date, timedelta

__all__ = ('NorthernIrelandFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class NorthernIrelandFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data from Northern Ireland Department of Health'''
    LOAD_PLUGIN = True
    SOURCE = 'GBR_NIDH'  # Northern Ireland Department of Health

    def excelreader(self, url):
        # read the excel file located at url

        testing_data = pd.read_excel(url, sheet_name='Tests', parse_dates=[0],
                                     names=['Sample_Date', 'LGD2014NAME', 'First Infections', 'Re-Infections',
                                            'Total Cases', 'Total Tests'])
        testing_data['LGD2014NAME'].fillna('Unknown', inplace=True)
        testing_data.replace('Missing Postcode', 'Unknown', inplace=True)

        deaths_data = pd.read_excel(url, sheet_name='Deaths', parse_dates=[0],
                                    names=['Date of Death', 'LGD', 'Setting', 'Gender', 'Age Band', 'Number of Deaths'])
        deaths_data['LGD'].fillna('Unknown', inplace=True)
        deaths_data.replace('Missing Postcode', 'Unknown', inplace=True)

        return testing_data, deaths_data

    def fetch(self):

        # Check the last four days and attempt to read a file with matching name
        # The name is not always the same. Check five variant filenames for each date.
        datetimeobj = datetime.today()
        attempts = 4
        while attempts > 0:
            day = datetimeobj.strftime('%d%m%y')
            # each possible filename inside a try-except
            try:
                url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-{day}_0.xlsx'
                return self.excelreader(url)
            except HTTPError:
                try:
                    url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-{day}.xlsx'
                    return self.excelreader(url)
                except HTTPError:
                    try:
                        url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-{day}.XLSX'
                        return self.excelreader(url)
                    except HTTPError:
                        try:
                            url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/dd-{day}.XLSX'
                            return self.excelreader(url)
                        except HTTPError:
                            try:
                                url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/dd-{day}.xlsx'
                                return self.excelreader(url)
                            except HTTPError:
                                # no filename has succeeded, check previous day and increment number of attempts
                                datetimeobj = datetimeobj - timedelta(days=1)
                                attempts = attempts - 1
        # if there is still no success report error
        logger.warning('Could not locate spreadsheet for GBR_NIDH. Check https://www.health-ni.gov.uk/articles/covid-19-daily-dashboard-updates to see if the filename has changed.')
        raise Exception

    def run(self):

        # fetch data from the online excel file
        testing_data, deaths_data = self.fetch()

        # now group the deaths data to remove detailed info and collapse different 'Unknowns'
        grouped = deaths_data.groupby(['Date of Death', 'LGD'], as_index=False)
        deaths_data = grouped[['Number of Deaths']].sum()
        deaths_data.rename(columns={'Date of Death': 'Date'}, inplace=True)

        # now group the testing data to collapse different 'Unknowns'
        grouped = testing_data.groupby(['Sample_Date', 'LGD2014NAME'], as_index=False)
        testing_data = grouped[['Total Tests', 'Total Cases']].sum()
        testing_data.rename(columns={'Sample_Date': 'Date', 'LGD2014NAME': 'LGD'}, inplace=True)

        # combine the two dataframes
        df = pd.merge(deaths_data, testing_data, how='outer', left_on=['Date', 'LGD'], right_on=['Date', 'LGD'])
        df.sort_values(['Date'], inplace=True)

        # pad out the dataframe with missing dates, then set cumulative sum
        idx = pd.MultiIndex.from_product([pd.date_range(df.iat[0, 0], df.iat[len(df) - 1, 0]),
                                          df.LGD.unique()], names=['Date', 'LGD'])
        df = df.set_index(['Date', 'LGD']).reindex(idx).fillna(0)
        df = pd.concat([df, df.groupby(level=1).cumsum().add_prefix('Cum_')], 1).sort_index(level=1).reset_index()

        # upsert the data
        for index, record in df.iterrows():

            date = record['Date']
            lgd = record['LGD']
            confirmed = record['Cum_Total Cases']
            tested = record['Cum_Total Tests']
            deaths = record['Cum_Number of Deaths']

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1='Northern Ireland',
                input_adm_area_2=lgd,
                input_adm_area_3=None,
                return_original_if_failure=True,
                suppress_exception=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': deaths,
                'tested': tested,
                'gid': gid
            }

            self.upsert_data(**upsert_obj)

            # do the upsert again for level 3

            if adm_area_2 != 'Unknown':
                upsert_obj['adm_area_3'] = adm_area_2
                upsert_obj['gid'] = gid
                self.upsert_data(**upsert_obj)

        # now collect info for all Northern Ireland
        grouped = df.groupby(['Date'], as_index=False)
        df = grouped[['Cum_Total Cases', 'Cum_Total Tests', 'Cum_Number of Deaths']].sum()

        # upsert the data
        for index, record in df.iterrows():
            date = record['Date']
            confirmed = record['Cum_Total Cases']
            tested = record['Cum_Total Tests']
            deaths = record['Cum_Number of Deaths']

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1='Northern Ireland',
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True,
                suppress_exception=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': deaths,
                'tested': tested,
                'gid': gid
            }

            self.upsert_data(**upsert_obj)
