import os
import csv
import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from .utils import get_country_codes, get_country_info

__all__ = ('GoogleMobilityFetcher',)

logger = logging.getLogger(__name__)


class GoogleMobilityFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch(self):
        # Google covid19 mobility data
        url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
        return pd.read_csv(url, low_memory=False)

    def get_region(self, countrycode: str, input_adm_area_1: str = None, input_adm_area_2: str = None,
                   input_adm_area_3: str = None):
        try:
            # Check if input data can be matched directly into administrative division table
            adm_area_1, adm_area_2, adm_area_3, gid = self.db.get_adm_division(
                countrycode, input_adm_area_1, input_adm_area_2, input_adm_area_3)
        except Exception as ex:
            adm_area_1, adm_area_2, adm_area_3, gid = None, None, None, None

        if not gid:
            # Check translate.csv for translation
            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                countrycode, input_adm_area_1, input_adm_area_2, input_adm_area_3)

        return adm_area_1, adm_area_2, adm_area_3, gid

    def run(self):
        data = self.fetch()
        country_codes = get_country_codes()

        unknown_regions = set()
        region_cache = dict()

        for index, record in data.iterrows():
            date = record['date']

            country, countrycode = get_country_info(country_codes,
                                                    country_a2_code=record['country_region_code'],
                                                    country_name=record['country_region'])

            if pd.isna(countrycode):
                logger.warning(f'Unable to process: {record}')
                continue

            input_adm_area_1 = record['sub_region_1'].strip() if pd.notna(record['sub_region_1']) else None
            input_adm_area_2 = record['sub_region_2'].strip() if pd.notna(record['sub_region_2']) else None

            if countrycode == 'USA':
                if input_adm_area_2:
                    input_adm_area_2 = input_adm_area_2.replace('County', '') \
                        .replace('Parish', '') \
                        .replace('St.', 'Saint').strip()
            elif countrycode == 'GBR' and input_adm_area_1:
                # Use adm_area_2 for Great Britain
                input_adm_area_2 = input_adm_area_1
                input_adm_area_1 = '%'
            elif input_adm_area_1:
                input_adm_area_1 = input_adm_area_1.replace('Province', '') \
                    .replace('District', '').replace('County', '') \
                    .replace('Region', '').replace('Governorate', '') \
                    .replace('State of', '').strip()

            key = (countrycode, input_adm_area_1, input_adm_area_2, '')

            # FOR DEBUGGING PURPOSE ONLY
            # if key in region_cache:
            #     continue

            if key in region_cache:
                adm_area_1, adm_area_2, adm_area_3, gid = region_cache.get(key)
            else:
                adm_area_1, adm_area_2, adm_area_3, gid = self.get_region(
                    countrycode, input_adm_area_1, input_adm_area_2, None)
                region_cache[key] = (adm_area_1, adm_area_2, adm_area_3, gid)

            if not gid:
                if key not in unknown_regions:
                    logger.warning(
                        f'Unable to find translation for: "{countrycode}", "{input_adm_area_1}", "{input_adm_area_2}" '
                        f'add correct translation in CSV file')
                unknown_regions.add(key)

            upsert_obj = {
                'source': 'GOOGLE_MOBILITY',
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
