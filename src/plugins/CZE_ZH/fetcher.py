import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime

__all__ = ('CzechiaFetcher',)

logger = logging.getLogger(__name__)

"""
    site-location: https://github.com/covid19-eu-zh/covid19-eu-data

    COVID19 data for European countries created and maintained by covid19-eu-zh

    Data originally from Czech Ministry of Health

    https://onemocneni-aktualne.mzcr.cz/covid-19
"""


class CzechiaFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch(self):
        url = 'https://github.com/covid19-eu-zh/covid19-eu-data/raw/master/dataset/covid-19-cz.csv'
        return pd.read_csv(url)

    def run(self):
        logger.info("Processing number of cases in Czechia")

        df = self.fetch()

        for index, record in df.iterrows():
            # date must be reformatted
            d = record['datetime']
            date = datetime.strptime(d, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')

            confirmed = int(record['cases']) if pd.notna(record['cases']) else None

            if pd.isna(record['nuts_3']):
                adm_area_1, adm_area_2, adm_area_3, gid = None, None, None, ['AUT']
            else:
                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=record['nuts_3'],
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                'source': 'COVID19-EU-ZH',
                'date': date,
                'country': 'Czechia',
                'countrycode': 'CZE',
                'adm_area_1': adm_area_1,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'gid': gid
            }

            self.db.upsert_epidemiology_data(**upsert_obj)
