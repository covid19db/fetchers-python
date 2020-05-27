from datetime import datetime
import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('WorldECDCFetcher',)

logger = logging.getLogger(__name__)


class WorldECDCFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'WRD_ECDC'

    def fetch(self):
        url = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'
        logger.debug('Fetching world confirmed cases, deaths data from ECDC')
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()
        data.sort_values(['countryterritoryCode', 'dateRep'])

        # Data contains new cases and deaths for each day. Get cumulative data by sorting by country
        # code and date, then reverse iterating (old to new) and adding to cumulative data for the same country
        country_total_confirmed_cases = dict()
        country_total_deaths = dict()

        for index, record in data[::-1].iterrows():
            # CSV file has format: dateRep,day,month,year,cases,deaths,geoId,continentExp,countryterritoryCode,
            # popData2018,countriesAndTerritories

            # Date has format 'DD/MM/YYYY'; need to convert it to 'YYYY-MM-DD' format before adding to database
            date_ddmmyyyy = record[0]
            date = datetime.strptime(date_ddmmyyyy, '%d/%m/%Y').strftime('%Y-%m-%d')

            country = record['countriesAndTerritories']
            country_code = record['countryterritoryCode']
            confirmed = int(record['cases'])
            dead = int(record['deaths'])

            total_confirmed = country_total_confirmed_cases.get(country_code, 0)
            total_confirmed = total_confirmed + confirmed
            country_total_confirmed_cases[country_code] = total_confirmed

            total_deaths = country_total_deaths.get(country_code, 0)
            total_deaths = total_deaths + dead
            country_total_deaths[country_code] = total_deaths

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': country,
                'countrycode': country_code,
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': [country_code],
                'confirmed': total_confirmed,
                'dead': total_deaths
            }

            self.db.upsert_epidemiology_data(**upsert_obj)
