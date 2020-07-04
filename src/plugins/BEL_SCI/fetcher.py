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
import math

__all__ = ('BEL_SCIFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class BEL_SCIFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'BEL_SCI'

    def region_fetch(self):
        logger.debug('Fetching region-level information')
        # a csv file to be downloaded
        # REGIONS are hard-coded, which was simple as there are only 3 provinces in Belgium

        confirmed_url = 'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv'
        raw_confirmed = pd.read_csv(confirmed_url)

        regions = np.array(['Brussels', 'Flanders', 'Wallonia'] * len(
            pd.date_range(raw_confirmed['DATE'].dropna().iloc[0], raw_confirmed['DATE'].dropna().iloc[-1])))
        dates = np.array([datetime.date(x).strftime('%Y-%m-%d') for x in
                          pd.date_range(raw_confirmed['DATE'].dropna().iloc[0],
                                        raw_confirmed['DATE'].dropna().iloc[-1]).tolist() for y in range(3)])
        cases = np.zeros(len(regions))
        empty_frame = pd.DataFrame(data=zip(dates, regions, cases), columns=['DATE', 'REGION', 'CASES'])

        dataframe = raw_confirmed[['DATE', 'REGION', 'CASES']].groupby(['DATE', 'REGION'], as_index=False).sum()
        confirmed = pd.merge(empty_frame, dataframe, on=['DATE', 'REGION'], how='left')[['DATE', 'REGION', 'CASES_y']].rename({'CASES_y': 'CASES'}, axis='columns')
        confirmed['CASES'] = confirmed['CASES'].fillna(0)
        confirmed = confirmed.groupby(['DATE', 'REGION']).sum().groupby(['REGION']).cumsum()

        hospitalised_url = 'https://epistat.sciensano.be/Data/COVID19BE_HOSP.csv'
        raw_hospitalised = pd.read_csv(hospitalised_url)

        regions = np.array(['Brussels', 'Flanders', 'Wallonia'] * len(
            pd.date_range(raw_hospitalised['DATE'].dropna().iloc[0], raw_hospitalised['DATE'].dropna().iloc[-1])))
        dates = np.array([datetime.date(x).strftime('%Y-%m-%d') for x in
                          pd.date_range(raw_hospitalised['DATE'].dropna().iloc[0],
                                        raw_hospitalised['DATE'].dropna().iloc[-1]).tolist() for y in range(3)])
        new_in = np.zeros(len(regions))
        empty_frame = pd.DataFrame(data=zip(dates, regions, new_in), columns=['DATE', 'REGION', 'NEW_IN'])

        dataframe = raw_hospitalised[['DATE', 'REGION', 'NEW_IN']].groupby(['DATE', 'REGION'], as_index=False).sum()

        hospitalised = pd.merge(empty_frame, dataframe, on=['DATE', 'REGION'], how='left')[
            ['DATE', 'REGION', 'NEW_IN_y']].rename({'NEW_IN_y': 'NEW_IN'}, axis='columns')
        hospitalised['NEW_IN'] = hospitalised['NEW_IN'].fillna(0)
        hospitalised = hospitalised.groupby(['DATE', 'REGION']).sum().groupby(['REGION']).cumsum()

        death_url = 'https://epistat.sciensano.be/Data/COVID19BE_MORT.csv'
        raw_death = pd.read_csv(death_url)

        regions = np.array(['Brussels', 'Flanders', 'Wallonia'] * len(
            pd.date_range(raw_death['DATE'].dropna().iloc[0], raw_death['DATE'].dropna().iloc[-1])))
        dates = np.array([datetime.date(x).strftime('%Y-%m-%d') for x in
                          pd.date_range(raw_death['DATE'].dropna().iloc[0],
                                        raw_death['DATE'].dropna().iloc[-1]).tolist() for y in range(3)])
        deaths = np.zeros(len(regions))
        empty_frame = pd.DataFrame(data=zip(dates, regions, deaths), columns=['DATE', 'REGION', 'DEATHS'])

        dataframe = raw_death[['DATE', 'REGION', 'DEATHS']].groupby(['DATE', 'REGION'], as_index=False).sum()

        deaths = pd.merge(empty_frame, dataframe, on=['DATE', 'REGION'], how='left')[
            ['DATE', 'REGION', 'DEATHS_y']].rename({'DEATHS_y': 'DEATHS'}, axis='columns')
        deaths['DEATHS'] = deaths['DEATHS'].fillna(0)
        deaths = deaths.groupby(['DATE', 'REGION']).sum().groupby(['REGION']).cumsum()

        data = confirmed.join(hospitalised, on=['DATE', 'REGION'], how='outer').join(deaths, on=['DATE', 'REGION'],
                                                                                     how='outer')
        return data

    def province_fetch(self):
        confirmed_url = 'https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv'
        raw_confirmed = pd.read_csv(confirmed_url)

        hospitalised_url = 'https://epistat.sciensano.be/Data/COVID19BE_HOSP.csv'
        raw_hospitalised = pd.read_csv(hospitalised_url)

        flanders_provinces = list(raw_confirmed[raw_confirmed['REGION'] == 'Flanders']['PROVINCE'].unique())
        wallonia_provinces = list(raw_confirmed[raw_confirmed['REGION'] == 'Wallonia']['PROVINCE'].unique())
        total_provinces = ['Brussels'] + flanders_provinces + wallonia_provinces

        regions = np.array(
            ['Brussels', 'Flanders', 'Flanders', 'Flanders', 'Flanders', 'Flanders', 'Wallonia', 'Wallonia', 'Wallonia',
             'Wallonia', 'Wallonia'] * len(
                pd.date_range(raw_confirmed['DATE'].dropna().iloc[0], raw_confirmed['DATE'].dropna().iloc[-1])))
        provinces = np.array(total_provinces * len(
            pd.date_range(raw_confirmed['DATE'].dropna().iloc[0], raw_confirmed['DATE'].dropna().iloc[-1])))
        dates = np.array([datetime.date(x).strftime('%Y-%m-%d') for x in
                          pd.date_range(raw_confirmed['DATE'].dropna().iloc[0],
                                        raw_confirmed['DATE'].dropna().iloc[-1]).tolist() for y in range(11)])
        cases = np.zeros(len(regions))
        empty_frame = pd.DataFrame(data=zip(dates, regions, provinces, cases),
                                   columns=['DATE', 'REGION', 'PROVINCE', 'CASES'])

        dataframe = raw_confirmed[['DATE', 'REGION', 'PROVINCE', 'CASES']].groupby(['DATE', 'REGION', 'PROVINCE'],
                                                                                   as_index=False).sum()
        confirmed = pd.merge(empty_frame, dataframe, on=['DATE', 'REGION', 'PROVINCE'], how='left')[
            ['DATE', 'REGION', 'PROVINCE', 'CASES_y']].rename({'CASES_y': 'CASES'}, axis='columns')
        confirmed['CASES'] = confirmed['CASES'].fillna(0)
        confirmed = confirmed.groupby(['DATE', 'REGION', 'PROVINCE']).sum().groupby(['REGION', 'PROVINCE']).cumsum()

        regions = np.array(
            ['Brussels', 'Flanders', 'Flanders', 'Flanders', 'Flanders', 'Flanders', 'Wallonia', 'Wallonia', 'Wallonia',
             'Wallonia', 'Wallonia'] * len(
                pd.date_range(raw_hospitalised['DATE'].dropna().iloc[0], raw_hospitalised['DATE'].dropna().iloc[-1])))
        provinces = np.array(total_provinces * len(
            pd.date_range(raw_hospitalised['DATE'].dropna().iloc[0], raw_hospitalised['DATE'].dropna().iloc[-1])))
        dates = np.array([datetime.date(x).strftime('%Y-%m-%d') for x in
                          pd.date_range(raw_hospitalised['DATE'].dropna().iloc[0],
                                        raw_hospitalised['DATE'].dropna().iloc[-1]).tolist() for y in range(11)])
        cases = np.zeros(len(regions))
        empty_frame = pd.DataFrame(data=zip(dates, regions, provinces, cases),
                                   columns=['DATE', 'REGION', 'PROVINCE', 'NEW_IN'])

        dataframe = raw_hospitalised[['DATE', 'REGION', 'PROVINCE', 'NEW_IN']].groupby(['DATE', 'REGION', 'PROVINCE'],
                                                                                       as_index=False).sum()
        hospitalised = pd.merge(empty_frame, dataframe, on=['DATE', 'REGION', 'PROVINCE'], how='left')[
            ['DATE', 'REGION', 'PROVINCE', 'NEW_IN_y']].rename({'NEW_IN_y': 'NEW_IN'}, axis='columns')

        hospitalised['NEW_IN'] = hospitalised['NEW_IN'].fillna(0)
        hospitalised = hospitalised.groupby(['DATE', 'REGION', 'PROVINCE']).sum().groupby(
            ['REGION', 'PROVINCE']).cumsum()

        data = confirmed.join(hospitalised, on=['DATE', 'REGION', 'PROVINCE'], how='outer')

        return data


    def run(self):
        region_data = self.region_fetch()

        for index, record in region_data.iterrows():
            # assumption is that the CSV file has the following columns:
            # date, province, confirmed cases, deaths, recoveries
            date = index[0]  # we expect date to be in YYYY-MM-DD format
            country = 'Belgium'
            province = index[1]
            confirmed = int(record[0]) if not np.isnan(record[0]) else None
            hospitalised = int(record[1]) if not np.isnan(record[1]) else None
            dead = int(record[2]) if not np.isnan(record[2]) else None

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=province,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                'source': self.SOURCE,
                # date is also mandatory, the format must be YYYY-MM-DD
                'date': date,
                # country is mandatory and should be in English
                # the exception is "Ships"
                'country': country,
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'BEL',
                # adm_area_1, when available, is a wide-area administrative region, like a
                # Canadian province in this case. There are also subareas adm_area_2 and
                # adm_area_3
                'adm_area_1': adm_area_1,
                # confirmed is the number of confirmed cases of infection, this is cumulative
                'confirmed': confirmed,
                # dead is the number of people who have died because of covid19, this is cumulative
                'dead': dead,
                # recovered is the number of people who have healed, this is cumulative
                #'recovered': recovered,
                # hospitalised is the number of people who needed hospitalisation, this is cumulative
                'hospitalised':hospitalised,

                'gid':gid
            }

            # see the main webpage or the README for all the available fields and their
            # semantics

            # update data
            self.upsert_data(**upsert_obj)
            # alternatively, we can issue the query directly using self.db.execute(query, data)
            # but use it with care!

        province_data=self.province_fetch()

        for index, record in province_data.iterrows():
            date = index[0]  # we expect date to be in YYYY-MM-DD format
            country = 'Belgium'
            region = index[1]
            province = index[2]

            confirmed = int(record[0]) if not np.isnan(record[0]) else None
            hospitalised = int(record[1]) if not np.isnan(record[1]) else None

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=region,
                input_adm_area_2=province,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': country,
                'countrycode': 'BEL',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'confirmed': confirmed,
                'hospitalised':hospitalised,
                'gid':gid
            }
            self.upsert_data(**upsert_obj)

