# Copyright University of Oxford 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('ExampleFetcher',)

logger = logging.getLogger(__name__)


class ExampleFetcher(AbstractFetcher):
    LOAD_PLUGIN = False
    SOURCE = 'CND_XXX'

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://someserver.com/path/source.csv'
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()

        for index, record in data.iterrows():
            # assumption is that the CSV file has the following columns:
            # date, province, confirmed cases, deaths, recoveries
            date = record[0]  # we expect date to be in YYYY-MM-DD format
            country = record[1]
            province = record[2]
            confirmed = int(record[3])
            dead = int(record[4])
            recovered = int(record[5])

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                'source': self.SOURCE,
                # date is also mandatory, the format must be YYYY-MM-DD
                'date': date,
                # country is mandatory and should be in English
                # the exception is "Ships"
                'country': country,
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'CDN',
                # adm_area_1, when available, is a wide-area administrative region, like a
                # Canadian province in this case. There are also subareas adm_area_2 and
                # adm_area_3
                'adm_area_1': province,
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
            self.db.upsert_epidemiology_data(**upsert_obj)

            # alternatively, we can issue the query directly using self.db.execute(query, data)
            # but use it with care!
