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

#
# Latin America Covid-19 Data Repository by DSRP
# https://github.com/DataScienceResearchPeru/covid-19_latinoamerica
#
# Note: only Brazil, Mexico, Ecuador, Peru, Colombia, Chile, Dominican Republic
# and Argentina are upserted
#

import time
import logging
import pandas as pd

__all__ = ('LatinAmericaDSRPFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class LatinAmericaDSRPFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'LAT_DSRP'

    def fetch(self, category):
        url = f'https://raw.githubusercontent.com/DataScienceResearchPeru/covid-19_latinoamerica/' \
              f'master/latam_covid_19_data/time_series/time_series_{category}.csv'
        return pd.read_csv(url,
                           index_col=['Country', 'Subdivision'],
                           usecols=lambda c: c not in ['ISO 3166-2 Code', 'Last Update']) \
            .stack() \
            .rename_axis(['Country', 'Subdivision', 'Date']) \
            .rename(category.title())

    def run(self):
        logger.debug('Fetching regional information')
        confirmed = self.fetch('confirmed')
        time.sleep(5)
        deaths = self.fetch('deaths')
        time.sleep(5)
        recovered = self.fetch('recovered')
        data = pd.concat([confirmed, deaths, recovered], axis=1).reset_index()

        for country in ['Brazil', 'Mexico', 'Ecuador', 'Peru', 'Colombia', 'Chile',
                        'Dominican Republic', 'Argentina']:
            for index, record in data[data['Country'] == country].iterrows():
                # country, subdivision, date, confirmed, deaths, recovered
                countrycode = country[:3].upper() if country != 'Chile' else 'CHL'
                subdivision = record[1]
                date = record[2]
                confirmed = int(record[3]) if pd.notna(record[3]) else None
                deaths = int(record[4]) if pd.notna(record[4]) else None
                recovered = int(record[5]) if pd.notna(record[5]) else None

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    country_code=countrycode,
                    input_adm_area_1=subdivision,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': country,
                    'countrycode': countrycode,
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'adm_area_3': adm_area_3,
                    'gid': gid,
                    'confirmed': confirmed,
                    'dead': deaths,
                    'recovered': recovered
                }
                self.upsert_data(**upsert_obj)
