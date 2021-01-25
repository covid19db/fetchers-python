# Copyright (C) 2020 University of Oxford
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
import math
from datetime import datetime

__all__ = ('CanadaFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class CanadaFetcher(BaseEpidemiologyFetcher):
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
            confirmed = int(record[5])

            if not math.isnan(record[7]):
                dead = int(record[7])
            else:
                dead = None

            if not math.isnan(record[9]):
                tested = int(record[9])
            else:
                tested = None

            if not math.isnan(record[10]):
                recovered = int(record[10])
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
                self.upsert_data(**upsert_obj)

            # alternatively, we can issue the query directly using self.db.execute(query, data)
            # but use it with care!
