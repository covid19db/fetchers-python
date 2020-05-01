import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from .utils import parser

__all__ = ('SpainWikiFetcher',)

logger = logging.getLogger(__name__)


class SpainWikiFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://raw.githubusercontent.com/victorvicpal/COVID19_es/master/data/final_data/dataCOVID19_es.csv'
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()

        # Parse data into the scheme of our database
        data = parser(data)
        for index, record in data.iterrows():
            date = record[0]  # we expect date to be in YYYY-MM-DD format
            adm_area_1 = record[1]
            confirmed = int(record[2]) if record[2] != '' else None
            recovered = int(record[3]) if record[3] != '' else None
            dead = int(record[4]) if record[4] != '' else None
            hospitalised = int(record[5]) if record[5] != '' else None
            hospitalised_icu = int(record[6]) if record[6] != '' else None

            upsert_obj = {
                'source': 'ESP_MSVP',
                'date': date,
                'country': 'Spain',
                'countrycode': 'ESP',
                'adm_area_1': adm_area_1,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,
                'hospitalised': hospitalised,
                'hospitalised_icu': hospitalised_icu
            }

            self.db.upsert_epidemiology_data(**upsert_obj)
