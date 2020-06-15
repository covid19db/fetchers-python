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
from utils.fetcher.abstract_fetcher import AbstractFetcher, FetcherType
from utils.country_codes_translator.translator import CountryCodesTranslator
from utils.fetcher.base_mobility import BaseMobilityFetcher
from utils.helper import remove_words
from .utils import get_recent_apple_mobility_data

__all__ = ('AppleMobilityFetcher',)

logger = logging.getLogger(__name__)


class AppleMobilityFetcher(BaseMobilityFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'APPLE_MOBILITY'

    @staticmethod
    def fix_countryname(countryname):
        if countryname == 'Republic of Korea':
            countryname = 'Korea, Republic of'
        elif countryname == 'Russia':
            countryname = 'Russian Federation'
        elif countryname == 'Taiwan':
            countryname = 'Taiwan, Province of China'
        elif countryname == 'Vietnam':
            countryname = 'Viet Nam'
        return countryname

    @staticmethod
    def fix_adm_division(country_codes_translator: CountryCodesTranslator, geo_type: str, region: str, sub_region: str,
                         country: str):
        if geo_type == 'country/region':
            adm_area_1, adm_area_2, countryname = None, None, region

        if geo_type == 'city':
            city = region
            adm_area_1 = sub_region
            countryname = country

            # TODO: Cover this case in the future
            # Skip city information
            return None, None, None, None

        if geo_type == 'sub-region':
            adm_area_1 = remove_words(region, words=['County', 'Region', 'Province', 'Prefecture'])
            adm_area_2, countryname = None, country

        if geo_type == 'county':
            adm_area_2 = remove_words(region, words=['County', 'Parish']) \
                .replace('St.', 'Saint').strip()
            adm_area_1, countryname = sub_region, country

        country, countrycode = country_codes_translator.get_country_info(
            country_name=AppleMobilityFetcher.fix_countryname(countryname))

        return country, countrycode, adm_area_1, adm_area_2

    def fetch(self):
        return get_recent_apple_mobility_data()

    def run(self):
        region_cache = dict()
        unknown_regions = set()

        data = self.fetch()

        time_list = list(data.columns)[6:]

        for index, record in data.iterrows():

            geo_type = record['geo_type']
            region = record['region']  # country/region, city, sub-region, county
            transportation_type = record['transportation_type']  # transit, walking, driving
            sub_region = record['sub-region']
            country = record['country']

            country, countrycode, input_adm_area_1, input_adm_area_2 = self.fix_adm_division(
                self.country_codes_translator, geo_type, region, sub_region, country)

            if countrycode is None:
                logger.warning(f'Unable to translate "{geo_type}": "{region}", '
                               f'"{sub_region}", "{country}"')
                continue

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
                        f'"{input_adm_area_1}", "{input_adm_area_2}"')
                unknown_regions.add(key)

            for k in range(len(time_list)):
                date = time_list[k]  # YYYY-MM-DD
                value = record[date]

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': country,
                    'countrycode': countrycode,
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'gid': gid
                }

                if transportation_type == 'transit':
                    upsert_obj['transit'] = value
                elif transportation_type == 'walking':
                    upsert_obj['walking'] = value
                elif transportation_type == 'driving':
                    upsert_obj['driving'] = value

                if gid:
                    self.upsert_data(**upsert_obj)

        # FOR DEBUGGING PURPOSE ONLY - save unknown regions into CSV file
        logger.warning('Unknown regions total: {}'.format(len(unknown_regions)))
        unknown_regions_list = sorted(list(unknown_regions), key=lambda x: (x[0] or '', x[1] or '', x[2] or ''))
        with open(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'unknown_regions_apple.csv'),
                  'w') as unknown_regions_file:
            csv_writer = csv.writer(unknown_regions_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for region in unknown_regions_list:
                csv_writer.writerow(list(region))
