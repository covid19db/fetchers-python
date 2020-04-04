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
            # assumption is that the CSV file has the following columns:
            # date, province, confirmed cases, deaths, recoveries
            date = record[0]  # we expect date to be in YYYY-MM-DD format
            adm_area_1 = record[1]
            confirmed = int(record[2]) if record[2] != '' else None
            recovered = int(record[3]) if record[3] != '' else None
            dead = int(record[4]) if record[4] != '' else None
            hospitalised = int(record[5]) if record[5] != '' else None
            hospitalised_icu = int(record[6]) if record[6] != '' else None


            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                'source': 'ES-MSW',
                # date is also mandatory, the format must be YYYY-MM-DD
                'date': date,
                # country is mandatory and should be in English
                # the exception is "Ships"
                'country': 'Spain',
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'ESP',
                # adm_area_1, when available, is a wide-area administrative region, like a
                # There are also subareas adm_area_2 and
                # adm_area_3
                'adm_area_1': adm_area_1,
                # confirmed is the number of confirmed cases of infection, this is cumulative
                'confirmed': confirmed,
                # dead is the number of people who have died because of covid19, this is cumulative
                'dead': dead,
                # recovered is the number of people who have healed, this is cumulative
                'recovered': recovered,
                # hospitalised is the number of people being treated
                'hospitalised': hospitalised,
                # hospitalised_icu is the number of critic cases
                'hospitalised_icu': hospitalised_icu
            }

            # see the main webpage or the README for all the available fields and their
            # semantics

            # the db object comes with a helper method that does the upsert for you:
            self.db.upsert_data(**upsert_obj)

            # alternatively, we can issue the query directly using self.db.execute(query, data)
            # but use it with care!
