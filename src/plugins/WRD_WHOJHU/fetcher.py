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

#
# Fetcher of world statistics from WHO
# as compiled by Johns Hopkins University
#

from datetime import datetime
import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from .utils import iso_alpha_3

__all__ = ('WorldWHOJHUFetcher',)

logger = logging.getLogger(__name__)


class WorldWHOJHUFetcher(AbstractFetcher):
    LOAD_PLUGIN = False
    SOURCE = 'WRD_WHOJHU'

    def fetch(self, category):
        logger.debug(f'Going to fetch world {category}')
        data = pd.read_csv(f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/'
                           f'master/csse_covid_19_data/csse_covid_19_time_series/'
                           f'time_series_covid19_{category}_global.csv')
        first_record = data.columns
        key = category if category != 'deaths' else 'dead'

        for index, record in data.iterrows():
            # CSV columns: Province/State, Country/Region, Lat, Long, date...
            province = record[0] if pd.notna(record[0]) else None
            country = record[1].replace('*', '')  # also remove accidental *
            # special cases:
            if province in ('Diamond Princess', 'Grand Princess', 'Recovered') or country in (
                    'Diamond Princess', 'MS Zaandam'):
                continue
            countrycode = iso_alpha_3[country]

            for col in range(4, len(record)):
                upsert_obj = {
                    'source': self.SOURCE,
                    'date': datetime.strptime(first_record[col], '%m/%d/%y').strftime('%Y-%m-%d'),
                    'country': country,
                    'countrycode': countrycode,
                    key: int(record[col]),
                    'gid': [countrycode]
                }

                if province is not None:
                    success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                        country_code=countrycode,
                        input_adm_area_1=province,
                        input_adm_area_2=None,
                        input_adm_area_3=None,
                        return_original_if_failure=True
                    )
                    upsert_obj['adm_area_1'] = adm_area_1
                    upsert_obj['adm_area_2'] = adm_area_2
                    upsert_obj['adm_area_3'] = adm_area_3
                    upsert_obj['gid'] = gid

                self.db.upsert_epidemiology_data(**upsert_obj)

    def run(self):
        self.fetch('confirmed')
        self.fetch('deaths')
        self.fetch('recovered')
