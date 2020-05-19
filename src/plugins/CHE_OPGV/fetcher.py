import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from .utils import parser

__all__ = ('SwitzerlandFetcher',)

logger = logging.getLogger(__name__)


class SwitzerlandFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://github.com/openZH/covid_19/raw/master/COVID19_Fallzahlen_CH_total.csv'
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()

        # Parse data into the scheme of our database
        data = parser(data)
        for index, record in data.iterrows():
            date = record[0]  # we expect date to be in YYYY-MM-DD format
            adm_area_1 = record[1]
            tested = int(record[2]) if record[2] != '' else None
            confirmed = int(record[3]) if record[3] != '' else None
            recovered = None #int(record[3]) if record[3] != '' else None   NOT PROVIDED IN THE CSV
            dead = int(record[4]) if record[4] != '' else None
            hospitalised = int(record[5]) if record[5] != '' else None
            hospitalised_icu = int(record[6]) if record[6] != '' else None

            upsert_obj = {
                'source': 'CHE_OPGOV',
                'date': date,
                'country': 'Switzerland',
                'countrycode': 'CHE',
                'adm_area_1': adm_area_1,
                'tested': tested,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,         #None NOT PROVIDED IN THE CSV
                'hospitalised': hospitalised,   
                'hospitalised_icu': hospitalised_icu
            }

            self.db.upsert_epidemiology_data(**upsert_obj)
