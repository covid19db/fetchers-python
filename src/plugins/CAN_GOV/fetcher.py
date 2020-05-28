import logging
import pandas as pd
import math
from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime

__all__ = ('CanadaFetcher',)

logger = logging.getLogger(__name__)


class CanadaFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'CAN_GOV'

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://health-infobase.canada.ca/src/data/covidLive/covid19.csv'
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()

        for index, record in data.iterrows():
            olddate = str(record[3])  # date is in dd-mm-yyyy format
            province = record[1]
            confirmed = int(record[4])

            if not math.isnan(record[6]):
                dead = int(record[6])
            else:
                dead = None

            if not math.isnan(record[8]):
                tested = int(record[8])
            else:
                tested = None

            if not math.isnan(record[9]):
                recovered = int(record[9])
            else:
                recovered = None

            # convert the date format to be in YYYY-MM-DD format as expected
            datetimeobject = datetime.strptime(olddate, '%d-%m-%Y')
            date = datetimeobject.strftime('%Y-%m-%d')

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=province,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            if province == 'Canada':
                adm_area_1 = None
                gid = ['CAN']

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                'source': self.SOURCE,
                # date is also mandatory, the format must be YYYY-MM-DD
                'date': date,
                # country is mandatory and should be in English
                # the exception is "Ships"
                'country': 'Canada',
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'CAN',
                # adm_area_1, when available, is a wide-area administrative region, like a
                # Canadian province in this case. This is left blank for the national figures.
                # Canada also lists 'Repatriated Travelers' which is a province for these figures.
                # This row is simply not added to the database
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid,
                'tested': tested,
                # confirmed is the number of confirmed cases of infection, this is cumulative
                'confirmed': confirmed,
                # dead is the number of people who have died because of covid19, this is cumulative
                'dead': dead,
                # recovered is the number of people who have healed, this is cumulative
                'recovered': recovered

            }

            # see the main webpage or the README for all the available fields and their
            # semantics

            # the db object comes with a helper method that does the upsert for you:
            if province != 'Repatriated travellers':
                self.db.upsert_epidemiology_data(**upsert_obj)

            # alternatively, we can issue the query directly using self.db.execute(query, data)
            # but use it with care!
