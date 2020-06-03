#
# COVID-19 Japan Anti-Coronavirus Dashboard
# https://github.com/code4sabae/covid19
#

import logging
import requests
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('JapanC1JACDFetcher',)

logger = logging.getLogger(__name__)


class JapanC1JACDFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'JPN_C1JACD'

    def fetch(self):
        return requests.get('https://raw.githubusercontent.com/code4sabae/covid19/'
                            'master/data/covid19japan-all.json').json()

    def get_or_na(self, dictionary, key):
        # '不明', '-' and None represent missing values
        value = dictionary.get(key)
        return value if value not in ('不明', '-') else None

    def run(self):
        logger.debug('Fetching all information')
        data = self.fetch()

        for record in data:
            date = record['lastUpdate']
            confirmed = self.get_or_na(record, 'npatients')
            recovered = self.get_or_na(record, 'nexits')
            dead = self.get_or_na(record, 'ndeaths')
            hospitalised = self.get_or_na(record, 'ncurrentpatients')
            tested = self.get_or_na(record, 'ninspections')
            hospitalised_icu = self.get_or_na(record, 'nheavycurrentpatients')

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'Japan',
                'countrycode': 'JPN',
                'tested': tested,
                'confirmed': confirmed,
                'recovered': recovered,
                'dead': dead,
                'hospitalised': hospitalised,
                'hospitalised_icu': hospitalised_icu,
                'gid': ['JPN']
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

            for area in record['area']:
                prefecture = area['name']
                confirmed = self.get_or_na(area, 'npatients')
                hospitalised = self.get_or_na(area, 'ncurrentpatients')
                recovered = self.get_or_na(area, 'nexits')
                dead = self.get_or_na(area, 'ndeaths')
                hospitalised_icu = self.get_or_na(area, 'nheavycurrentpatients')
                tested = self.get_or_na(area, 'ninspections')

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    country_code='JPN',
                    input_adm_area_1=prefecture,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'Japan',
                    'countrycode': 'JPN',
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
