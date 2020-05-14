#
# DS4C: Data Science for COVID-19 in South Korea
# https://github.com/jihoo-kim/Data-Science-for-COVID-19
#

import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('SouthKoreaDS4CFetcher',)

logger = logging.getLogger(__name__)


class SouthKoreaDS4CFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch(self):
        url = 'https://raw.githubusercontent.com/jihoo-kim/Data-Science-for-COVID-19/master/dataset/Time/Time.csv'
        return pd.read_csv(url)

    def fetch_province(self):
        url = 'https://raw.githubusercontent.com/jihoo-kim/Data-Science-for-COVID-19/master/dataset/Time/TimeProvince.csv'
        return pd.read_csv(url)

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch()

        for index, record in data.iterrows():
            # date, time, test, negative, confirmed, released, deceased
            date = record[0]
            test = int(record[2])
            confirmed = int(record[4])
            released = int(record[5])
            deceased = int(record[6])

            upsert_obj = {
                'source': 'KOR_DS4C',
                'date': date,
                'country': 'South Korea',
                'countrycode': 'KOR',
                'gid': ['KOR'],
                'tested': test,
                'confirmed': confirmed,
                'dead': deceased,
                'recovered': released
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug('Fetching regional information')
        data = self.fetch_province()

        for index, record in data.iterrows():
            # date, time, province, confirmed, released, deceased
            date = record[0]
            province = record[2]
            confirmed = int(record[3])
            released = int(record[4])
            deceased = int(record[5])

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=province,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': 'KOR_DS4C',
                'date': date,
                'country': 'South Korea',
                'countrycode': 'KOR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid,
                'confirmed': confirmed,
                'dead': deceased,
                'recovered': released
            }
            self.db.upsert_epidemiology_data(**upsert_obj)
