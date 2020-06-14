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

#
# COVID19-India API
# https://api.covid19india.org/
#
# Note: Ladakh is not in the GADM database
#

import logging
import pandas as pd
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher
from .utils import iso_3166_2_in

__all__ = ('IndiaCOVINDFetcher',)

logger = logging.getLogger(__name__)


class IndiaCOVINDFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'IND_COVIND'

    def fetch_cases(self):
        url = 'https://api.covid19india.org/csv/latest/case_time_series.csv'
        return pd.read_csv(url,
                           index_col=0,
                           usecols=[0, 2, 4, 6],
                           parse_dates=[0],
                           date_parser=lambda d: pd.to_datetime(d + '2020', format='%d %B %Y'))

    def fetch_tested(self):
        url = 'https://api.covid19india.org/csv/latest/tested_numbers_icmr_data.csv'

        # Return the last update on each day
        return pd.read_csv(url,
                           index_col=0,
                           usecols=[0, 1],
                           parse_dates=[0],
                           date_parser=lambda d: pd.to_datetime(d[:10], format='%d/%m/%Y')) \
            .dropna() \
            .groupby(level=0) \
            .last()

    def fetch_state_cases(self):
        url = 'https://api.covid19india.org/csv/latest/state_wise_daily.csv'

        # Return cumulative sums for each state
        return pd.read_csv(url,
                           index_col=[0, 1],
                           usecols=[c for c in range(40) if c != 2],
                           parse_dates=[0],
                           date_parser=lambda d: pd.to_datetime(d, format='%d-%b-%y')) \
            .rename(columns=iso_3166_2_in) \
            .unstack(level=1) \
            .apply(pd.Series.cumsum) \
            .stack(level=0)

    def fetch_state_tested(self):
        url = 'https://api.covid19india.org/csv/latest/statewise_tested_numbers_data.csv'

        # Return the last update on each day for each state
        return pd.read_csv(url,
                           index_col=[0, 1],
                           usecols=[0, 1, 2],
                           parse_dates=[0],
                           date_parser=lambda d: pd.to_datetime(d, format='%d/%m/%Y')) \
            .dropna() \
            .groupby(level=[0, 1]) \
            .last()

    def run(self):
        logger.debug('Fetching country-level information')
        cases = self.fetch_cases()
        tested = self.fetch_tested()
        data = cases.join(tested)

        for index, record in data.iterrows():
            # confirmed, recovered, deceased, samples_tested
            date = index.strftime('%Y-%m-%d')
            confirmed = int(record[0])
            recovered = int(record[1])
            deceased = int(record[2])
            samples_tested = int(record[3]) if pd.notna(record[3]) else None

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'India',
                'countrycode': 'IND',
                'gid': ['IND'],
                'tested': samples_tested,
                'confirmed': confirmed,
                'dead': deceased,
                'recovered': recovered
            }
            self.upsert_data(**upsert_obj)

        logger.debug('Fetching regional information')
        state_cases = self.fetch_state_cases().rename_axis(['Date', 'State'])
        state_tested = self.fetch_state_tested().rename_axis(['Date', 'State'])
        data = state_cases.join(state_tested).reset_index()

        for index, record in data.iterrows():
            # date, state, confirmed, deceased, recovered, tested
            date = record[0].strftime('%Y-%m-%d')
            state = record[1]
            confirmed = int(record[2]) if pd.notna(record[2]) else None
            deceased = int(record[3]) if pd.notna(record[3]) else None
            recovered = int(record[4]) if pd.notna(record[4]) else None
            tested = int(record[5]) if pd.notna(record[5]) else None

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'India',
                'countrycode': 'IND',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid if success else [],
                'tested': tested,
                'confirmed': confirmed,
                'dead': deceased,
                'recovered': recovered
            }
            self.upsert_data(**upsert_obj)
