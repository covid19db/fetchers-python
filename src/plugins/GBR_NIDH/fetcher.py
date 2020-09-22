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
                                     names=['Date', 'LGD', 'Tests', 'Individuals', 'Positives'])
        testing_data['LGD'].fillna('Unknown', inplace=True)
        testing_data.replace('Missing Postcode', 'Unknown', inplace=True)

        deaths_data = pd.read_excel(url, sheet_name='Deaths', parse_dates=[0],
                                    names=['Date', 'LGD', 'Setting', 'Gender', 'Age Band', 'Number of Deaths'])
        deaths_data['LGD'].fillna('Unknown', inplace=True)
        deaths_data.replace('Missing Postcode', 'Unknown', inplace=True)

        return testing_data, deaths_data

    def fetch(self):

        # Check the last four days and attempt to read a file with matching name
        # The name is, sadly, not always the same. Check four variants for each date.
        datetimeobj = datetime.today()
        attempts = 4
        while attempts > 0:
            try:
                day = datetimeobj.strftime('%d%m%y')
                url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-{day}.xlsx'
                testing_data, deaths_data = self.excelreader(url)
                return testing_data, deaths_data
            except:
                try:
                    url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-{day}.XLSX'
                    testing_data, deaths_data = self.excelreader(url)
                    return testing_data, deaths_data
                except:
                    try:
                        url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/dd-{day}.XLSX'
                        testing_data, deaths_data = self.excelreader(url)
                        return testing_data, deaths_data
                    except:
                        try:
                            url = f'https://www.health-ni.gov.uk/sites/default/files/publications/health/dd-{day}.xlsx'
                            testing_data, deaths_data = self.excelreader(url)
                            return testing_data, deaths_data
                        except:
                            datetimeobj = datetimeobj - timedelta(days=1)
                            attempts = attempts - 1

    def run(self):

        # fetch data from the online excel file
        testing_data, deaths_data = self.fetch()

        # now group the deaths data to remove detailed info and collapse different 'Unknowns'
        grouped = deaths_data.groupby(['Date', 'LGD'], as_index=False)
        deaths_data = grouped[['Number of Deaths']].sum()

        # now group the testing data to collapse different 'Unknowns'
        grouped = testing_data.groupby(['Date', 'LGD'], as_index=False)
        testing_data = grouped[['Individuals', 'Positives']].sum()

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
            confirmed = record['Cum_Positives']
            tested = record['Cum_Individuals']
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
        df = grouped[['Cum_Positives', 'Cum_Individuals', 'Cum_Number of Deaths']].sum()

        # upsert the data
        for index, record in df.iterrows():
            date = record['Date']
            confirmed = record['Cum_Positives']
            tested = record['Cum_Individuals']
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
