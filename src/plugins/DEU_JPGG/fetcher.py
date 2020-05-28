#
# Germany, data collected by Jan-Philip Gehrcke, from the
# Public Health Offices (Gesundheitsaemter) in Germany
# https://gehrcke.de/2020/03/covid-19-sars-cov-2-resources/
# github: https://github.com/jgehrcke/covid-19-germany-gae
#

import logging
import requests
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('GermanyJPGGFetcher',)

logger = logging.getLogger(__name__)


class GermanyJPGGFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'DEU_JPGG'

    def fetch(self, state):
        logger.debug(f'Fetching cases for Germany region {state}')
        data = requests.get(f'https://covid19-germany.appspot.com/timeseries/{state}/cases').json()

        for record in data['data']:
            date = next(iter(record))

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='DEU',
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date[:10],
                'country': 'Germany',
                'countrycode': 'DEU',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'confirmed': int(record[date]),
                'gid': gid
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug(f'Fetching deaths for Germany region {state}')
        data = requests.get(f'https://covid19-germany.appspot.com/timeseries/{state}/deaths').json()

        for record in data['data']:
            date = next(iter(record))

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='DEU',
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date[:10],
                'country': 'Germany',
                'countrycode': 'DEU',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'dead': int(record[date]),
                'gid': gid
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

    def run(self):
        for state in self.adm_translator.translation_pd['input_adm_area_1']:
            self.fetch(state)
