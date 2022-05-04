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

__all__ = ('WorldECDCFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)

continents_codes = {
    'Asia': 'ABB',
    'Europe': 'EEE',
    'Africa': 'FFF',
    'America': 'AME',
    'North America': 'NNN',
    'South America': 'SRR',
    'Oceania': 'UUU',
    'NATO countries': 'NTT',
    'Other continent': '---'
}


class WorldECDCFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'WRD_ECDC_WEEKLY'

    def map_country_code(self, country_code):
        if country_code == 'CNG1925':
            return 'TWN'
        if country_code == 'MSF':
            return 'MSR'
        if country_code == 'XKX':
            return 'XKO'
        return country_code

    def fetch(self):
        url = 'https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/csv/data.csv'
        logger.debug('Fetching world confirmed cases, deaths data from ECDC')
        # All data logged as the Thursday of that week as per
        # https://www.ecdc.europa.eu/en/covid-19/data-collection
        dateparse = lambda x: datetime.strptime(x + '-4', '%Y-%W-%w')
        return pd.read_csv(url, parse_dates=['year_week'], date_parser=dateparse)

    def run(self):
        rawdata = self.fetch()
        data = pd.pivot_table(rawdata, values='cumulative_count', index=['country_code', 'continent', 'year_week'],
                              columns=['indicator'], aggfunc=np.sum).reset_index()

        for index, record in data.iterrows():

            # Date formatted correctly during read_csv
            date = record['year_week']

            # country = record['countriesAndTerritories']
            country_code = record['country_code']
            if pd.isna(country_code):
                continue

            country, adm_area_1, adm_area_2, adm_area_3, gid = self.data_adapter.get_adm_division(country_code)
            confirmed = int(record['cases'])
            dead = int(record['deaths'])

            if gid is None:
                logger.error(f'No GID for : {country_code}')

            upsert_obj = {
                'source': self.SOURCE,
                'date': date.strftime('%Y-%m-%d'),
                'country': country,
                'countrycode': country_code,
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': [country_code],
                'confirmed': confirmed,
                'dead': dead
            }
            self.upsert_data(**upsert_obj)

        # now group by continent
        grouped = data.groupby(['year_week', 'continent'], as_index=False)
        continentaldf = grouped[['cumulative_count']].sum()

        for index, record in continentaldf.iterrows():
            date = record['date']
            continent = 'Other continent' if record['continent'] == 'Other' else record['continent']
            confirmed = int(record['cases'])
            dead = int(record['deaths'])

            continent_code = continents_codes.get(continent)

            upsert_obj = {
                'source': self.SOURCE,
                'date': date.strftime('%Y-%m-%d'),
                'country': continent,
                'countrycode': continent_code,
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': None,
                'confirmed': confirmed,
                'dead': dead
            }
            self.upsert_data(**upsert_obj)

        # finally a global figure
        # same method but just sort by the date

        grouped = continentaldf.groupby(['year_week'], as_index=False)
        globaldf = grouped[['cumulative_count']].sum()

        for index, record in globaldf.iterrows():
            date = record['year_week']
            confirmed = int(record['cases'])
            dead = int(record['deaths'])

            upsert_obj = {
                'source': self.SOURCE,
                'date': date.strftime('%Y-%m-%d'),
                'country': 'World',
                'countrycode': 'WRD',
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': None,
                'confirmed': confirmed,
                'dead': dead
            }
            self.upsert_data(**upsert_obj)
