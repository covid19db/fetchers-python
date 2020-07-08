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
    SOURCE = 'WRD_ECDC'

    def map_country_code(self, country_code):
        if country_code == 'CNG1925':
            return 'TWN'
        if country_code == 'MSF':
            return 'MSR'
        if country_code == 'XKX':
            return 'XKO'
        return country_code

    def fetch(self):
        url = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'
        logger.debug('Fetching world confirmed cases, deaths data from ECDC')
        dateparse = lambda x: datetime.strptime(x, '%d/%m/%Y')
        return pd.read_csv(url, parse_dates=['dateRep'], date_parser=dateparse)

    def run(self):
        data = self.fetch()
        data.sort_values(['countryterritoryCode', 'dateRep'], inplace=True)

        # Data contains new cases and deaths for each day. Get cumulative data by sorting by country
        # code and date, then iterating and adding to cumulative data for the same country
        country_total_confirmed_cases = dict()
        country_total_deaths = dict()

        for index, record in data.iterrows():
            # CSV file has format: dateRep,day,month,year,cases,deaths,geoId,continentExp,countryterritoryCode,
            # popData2018,countriesAndTerritories

            # Date formatted correctly during read_csv
            date = record[0]

            # country = record['countriesAndTerritories']
            country_code = self.map_country_code(record['countryterritoryCode'])
            if pd.isna(country_code):
                continue

            country, adm_area_1, adm_area_2, adm_area_3, gid = self.data_adapter.get_adm_division(country_code)
            confirmed = int(record['cases'])
            dead = int(record['deaths'])

            total_confirmed = country_total_confirmed_cases.get(country_code, 0)
            total_confirmed = total_confirmed + confirmed
            country_total_confirmed_cases[country_code] = total_confirmed

            total_deaths = country_total_deaths.get(country_code, 0)
            total_deaths = total_deaths + dead
            country_total_deaths[country_code] = total_deaths

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
                'confirmed': total_confirmed,
                'dead': total_deaths
            }
            self.upsert_data(**upsert_obj)

        # now group by continent
        grouped = data.groupby(['dateRep', 'continentExp'], as_index=False)
        continentaldf = grouped[['cases', 'deaths']].sum()
        continentaldf.sort_values(['continentExp', 'dateRep'], inplace=True)

        # Data contains new cases and deaths for each day. Get cumulative data by sorting by continent
        # and date, then iterating and adding to cumulative data for the same continent
        # just use same dictionaries as for countries

        for index, record in continentaldf.iterrows():
            date = record['dateRep']
            continent = 'Other continent' if record['continentExp'] == 'Other' else record['continentExp']
            confirmed = record['cases']
            dead = record['deaths']

            total_confirmed = country_total_confirmed_cases.get(continent, 0)
            total_confirmed = total_confirmed + confirmed
            country_total_confirmed_cases[continent] = total_confirmed

            total_deaths = country_total_deaths.get(continent, 0)
            total_deaths = total_deaths + dead
            country_total_deaths[continent] = total_deaths

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
                'confirmed': total_confirmed,
                'dead': total_deaths
            }
            self.upsert_data(**upsert_obj)
