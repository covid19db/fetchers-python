#
# Latin America Covid-19 Data Repository by DSRP
# https://github.com/DataScienceResearchPeru/covid-19_latinoamerica
#
# Note: only Brazil, Mexico, Ecuador, Peru, Colombia, Chile, Dominican Republic
# and Argentina are upserted
#

import time
import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('LatinAmericaDSRPFetcher',)

logger = logging.getLogger(__name__)


class LatinAmericaDSRPFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'LAT_DSRP'

    def fetch_confirmed(self):
        url = 'https://raw.githubusercontent.com/DataScienceResearchPeru/covid-19_latinoamerica/' \
              'master/latam_covid_19_data/time_series/time_series_confirmed.csv'
        return pd.read_csv(url,
                           index_col=['Country', 'Subdivision'],
                           usecols=lambda c: c not in ['ISO 3166-2 Code', 'Last Update']) \
            .stack() \
            .rename_axis(['Country', 'Subdivision', 'Date']) \
            .rename('Confirmed')

    def fetch_deaths(self):
        url = 'https://raw.githubusercontent.com/DataScienceResearchPeru/covid-19_latinoamerica/' \
              'master/latam_covid_19_data/time_series/time_series_deaths.csv'
        return pd.read_csv(url,
                           index_col=['Country', 'Subdivision'],
                           usecols=lambda c: c not in ['ISO 3166-2 Code', 'Last Update']) \
            .stack() \
            .rename_axis(['Country', 'Subdivision', 'Date']) \
            .rename('Deaths')

    def fetch_recovered(self):
        url = 'https://raw.githubusercontent.com/DataScienceResearchPeru/covid-19_latinoamerica/' \
              'master/latam_covid_19_data/time_series/time_series_recovered.csv'
        return pd.read_csv(url,
                           index_col=['Country', 'Subdivision'],
                           usecols=lambda c: c not in ['ISO 3166-2 Code', 'Last Update']) \
            .stack() \
            .rename_axis(['Country', 'Subdivision', 'Date']) \
            .rename('Recovered')

    def run(self):
        logger.debug('Fetching regional information')
        confirmed = self.fetch_confirmed()
        time.sleep(5)
        deaths = self.fetch_deaths()
        time.sleep(5)
        recovered = self.fetch_recovered()
        data = pd.concat([confirmed, deaths, recovered], axis=1).reset_index()

        for country in ['Brazil', 'Mexico', 'Ecuador', 'Peru', 'Colombia', 'Chile',
                        'Dominican Republic', 'Argentina']:
            for index, record in data[data['Country'] == country].iterrows():
                # country, subdivision, date, confirmed, deaths, recovered
                countrycode = country[:3].upper() if country != 'Chile' else 'CHL'
                subdivision = record[1]
                date = record[2]
                confirmed = int(record[3]) if pd.notna(record[3]) else None
                deaths = int(record[4]) if pd.notna(record[4]) else None
                recovered = int(record[5]) if pd.notna(record[5]) else None

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    country_code=countrycode,
                    input_adm_area_1=subdivision,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': country,
                    'countrycode': countrycode,
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'adm_area_3': adm_area_3,
                    'gid': gid,
                    'confirmed': confirmed,
                    'dead': deaths,
                    'recovered': recovered
                }
                self.db.upsert_epidemiology_data(**upsert_obj)
