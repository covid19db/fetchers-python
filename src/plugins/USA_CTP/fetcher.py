#
# The COVID Tracking Project
# https://covidtracking.com/api
#

from datetime import datetime
import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('UnitedStatesCTPFetcher',)

logger = logging.getLogger(__name__)


class UnitedStatesCTPFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'USA_CTP'

    def fetch(self, category, usecols):
        return pd.read_csv(f'https://covidtracking.com/api/v1/{category}/daily.csv',
                           usecols=usecols)

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch('us',
                          ['date',
                           'positive',
                           'hospitalizedCumulative',
                           'inIcuCumulative',
                           'recovered',
                           'death',
                           'totalTestResults'])

        for index, record in data.iterrows():
            date = datetime.strptime(str(int(record[0])), '%Y%m%d').strftime('%Y-%m-%d')
            confirmed = int(record[1]) if pd.notna(record[1]) else None
            hospitalised = int(record[2]) if pd.notna(record[2]) else None
            hospitalised_icu = int(record[3]) if pd.notna(record[3]) else None
            recovered = int(record[4]) if pd.notna(record[4]) else None
            dead = int(record[5]) if pd.notna(record[5]) else None
            tested = int(record[6]) if pd.notna(record[6]) else None

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United States',
                'countrycode': 'USA',
                'tested': tested,
                'confirmed': confirmed,
                'recovered': recovered,
                'dead': dead,
                'hospitalised': hospitalised,
                'hospitalised_icu': hospitalised_icu,
                'gid': ['USA']
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug('Fetching regional information')
        data = self.fetch('states',
                          ['date',
                           'state',
                           'positive',
                           'hospitalizedCumulative',
                           'inIcuCumulative',
                           'recovered',
                           'death',
                           'totalTestResults'])

        for index, record in data.iterrows():
            date = datetime.strptime(str(int(record[0])), '%Y%m%d').strftime('%Y-%m-%d')
            state = record[1]
            confirmed = int(record[2]) if pd.notna(record[2]) else None
            hospitalised = int(record[3]) if pd.notna(record[3]) else None
            hospitalised_icu = int(record[4]) if pd.notna(record[4]) else None
            recovered = int(record[5]) if pd.notna(record[5]) else None
            dead = int(record[6]) if pd.notna(record[6]) else None
            tested = int(record[7]) if pd.notna(record[7]) else None

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='USA',
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United States',
                'countrycode': 'USA',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'tested': tested,
                'confirmed': confirmed,
                'recovered': recovered,
                'dead': dead,
                'hospitalised': hospitalised,
                'hospitalised_icu': hospitalised_icu,
                'gid': gid
            }
            self.db.upsert_epidemiology_data(**upsert_obj)
