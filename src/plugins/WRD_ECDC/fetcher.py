from datetime import datetime
import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('WorldECDCFetcher',)

logger = logging.getLogger(__name__)


class WorldECDCFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch(self):
        url = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'
        logger.debug('Fetching world confirmed cases, deaths data from ECDC')
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()
        data.sort_values(['countryterritoryCode', 'dateRep'])

        # Data contains new cases and deaths for each day. Get cumulative data by sorting by country
        # code and date, then reverse iterating (old to new) and adding to cumulative data for the same country
        last_country_code = None
        last_country_total_confirmed_cases = 0
        last_country_total_deaths = 0

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

            if last_country_code is None or last_country_code != country_code:
                # New country so reset counters
                last_country_total_confirmed_cases = confirmed
                last_country_total_deaths = dead
                last_country_code = country_code
            else:
                last_country_total_confirmed_cases += confirmed
                last_country_total_deaths += dead

            upsert_obj = {
                'source': 'WRD_ECDC',
                'date': date,
                'country': country,
                'countrycode': country_code,
                'confirmed': last_country_total_confirmed_cases,
                'dead': last_country_total_deaths
            }

            self.db.upsert_epidemiology_data(**upsert_obj)
