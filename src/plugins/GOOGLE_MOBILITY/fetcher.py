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

import os
import csv
import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher, FetcherType

__all__ = ('GoogleMobilityFetcher',)

from utils.helper import remove_words

logger = logging.getLogger(__name__)


class GoogleMobilityFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    TYPE = FetcherType.MOBILITY
    SOURCE = 'GOOGLE_MOBILITY'

    def fetch(self):
        # Google covid19 mobility data
        url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
        return pd.read_csv(url, low_memory=False)

    def run(self):
        data = self.fetch()

        unknown_regions = set()
        region_cache = dict()

        for index, record in data.iterrows():
            date = record['date']

            country, countrycode = self.country_codes_translator.get_country_info(
                country_a2_code=record['country_region_code'],
                country_name=record['country_region'])

            if pd.isna(countrycode):
                logger.warning(f'Unable to process: {record}')
                continue

            input_adm_area_1 = record['sub_region_1'].strip() if pd.notna(record['sub_region_1']) else None
            input_adm_area_2 = record['sub_region_2'].strip() if pd.notna(record['sub_region_2']) else None

            if countrycode == 'USA':
                if input_adm_area_2:
                    input_adm_area_2 = remove_words(input_adm_area_2, words=['County', 'Parish']) \
                        .replace('St.', 'Saint').strip()
            elif countrycode == 'GBR' and input_adm_area_1:
                # Use adm_area_2 for Great Britain
                input_adm_area_2 = input_adm_area_1
                input_adm_area_1 = '%'
            elif countrycode == 'JAM' and input_adm_area_1:
                input_adm_area_1 = remove_words(input_adm_area_1, words=['Parish']) \
                    .replace('St.', 'Saint').strip()
            elif input_adm_area_1:
                input_adm_area_1 = remove_words(
                    input_adm_area_1,
                    words=['Province', 'District', 'County', 'Region', 'Governorate', 'State of', 'Department'])

            key = (countrycode, input_adm_area_1, input_adm_area_2, '')

            # FOR DEBUGGING PURPOSE ONLY
            # if key in region_cache:
            #     continue

            if key in region_cache:
                adm_area_1, adm_area_2, adm_area_3, gid = region_cache.get(key)
            else:
                adm_area_1, adm_area_2, adm_area_3, gid = self.get_region(
                    countrycode, input_adm_area_1, input_adm_area_2,
                    suppress_exception=True)
                region_cache[key] = (adm_area_1, adm_area_2, adm_area_3, gid)

            if not gid:
                if key not in unknown_regions:
                    logger.warning(
                        f'Unable to find translation for: "{countrycode}", '
                        f'"{input_adm_area_1}", "{input_adm_area_2}" ')
                unknown_regions.add(key)

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': country,
                'countrycode': countrycode,
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'gid': gid,
                'transit_stations': record['transit_stations_percent_change_from_baseline'],
                'residential': record['residential_percent_change_from_baseline'],
                'workplace': record['workplaces_percent_change_from_baseline'],
                'parks': record['parks_percent_change_from_baseline'],
                'retail_recreation': record['retail_and_recreation_percent_change_from_baseline'],
                'grocery_pharmacy': record['residential_percent_change_from_baseline']
            }

            if gid:
                self.db.upsert_mobility_data(**upsert_obj)

        # FOR DEBUGGING PURPOSE ONLY - save unknown regions into CSV file
        logger.warning('Unknown regions total: {}'.format(len(unknown_regions)))
        unknown_regions_list = sorted(list(unknown_regions), key=lambda x: (x[0] or '', x[1] or '', x[2] or ''))
        with open(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'unknown_regions.csv'),
                  'w') as unknown_regions_file:
            csv_writer = csv.writer(unknown_regions_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for region in unknown_regions_list:
                csv_writer.writerow(list(region))
