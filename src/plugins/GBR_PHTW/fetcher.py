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
# Coronavirus (COVID-19) UK Historical Data
# https://github.com/tomwhite/covid-19-uk-data
#
# Notes:
# For England, adm_area_2 is upper tier local authority
# For Scotland and Wales, adm_area_2 is health board
# For Northern Ireland, adm_area_2 is local government district
#

import logging
import pandas as pd

__all__ = ('UnitedKingdomPHTWFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class UnitedKingdomPHTWFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'GBR_PHTW'

    def fetch(self):
        url = 'https://raw.githubusercontent.com/tomwhite/covid-19-uk-data/master/data/covid-19-indicators-uk.csv'

        # Return pivoted data
        return pd.read_csv(url) \
            .set_index(['Date', 'Country', 'Indicator']) \
            .unstack('Indicator') \
            .reset_index()

    def fetch_area(self):
        url = 'https://raw.githubusercontent.com/tomwhite/covid-19-uk-data/master/data/covid-19-cases-uk.csv'
        return pd.read_csv(url)

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch()

        for index, record in data.iterrows():
            # date, country, confirmedcases, deaths, tests
            date = record[0]
            country = record[1]
            confirmedcases = int(record[2]) if pd.notna(record[2]) else None
            deaths = int(record[3]) if pd.notna(record[3]) else None
            tests = int(record[4]) if pd.notna(record[4]) else None

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=country,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'tested': tests,
                'confirmed': confirmedcases,
                'dead': deaths,
                'gid': gid
            }

            if adm_area_1=='Northern Ireland':
                break

            self.upsert_data(**upsert_obj)

        logger.debug('Fetching regional information')
        logger.warning('Scotland and Wales report by health board - GIDs are approximations by local authorities')
        data = self.fetch_area()

        for index, record in data.iterrows():
            # date, country, areacode, area, totalcases
            if pd.notna(record[2]):
                date = record[0]
                country = record[1]
                area = record[3]
                totalcases = int(record[4]) if pd.notna(record[4]) else None

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=country,
                    input_adm_area_2=area,
                    input_adm_area_3=None,
                    return_original_if_failure=True
                )

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'United Kingdom',
                    'countrycode': 'GBR',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'confirmed': totalcases,
                    'gid': gid
                }

                if adm_area_1 == 'Northern Ireland':
                    break

                self.upsert_data(**upsert_obj)
