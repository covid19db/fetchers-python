import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

from .utils import get_country_codes

__all__ = ('GoogleMobilityFetcher',)

logger = logging.getLogger(__name__)


class GoogleMobilityFetcher(AbstractFetcher):
    LOAD_PLUGIN = False

    def fetch(self):
        # Google covid19 mobility data
        url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()
        country_codes = get_country_codes()

        for index, record in data.iterrows():
            date = record['date']
            country_a2_code = record['country_region_code']

            country_info = country_codes[country_codes['Alpha-2 code'] == country_a2_code].to_dict('records')[0]
            country_a3_code = country_info["Alpha-3 code"]
            country_name = country_info["English short name lower case"]

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=record['sub_region_1'],
                input_adm_area_2=record['sub_region_2'],
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            # Upsert to mobility
            upsert_obj = {
                'source': 'GOOGLE_MOBILITY',
                'date': date,
                'country': country_name,
                'countrycode': country_a3_code,
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

            self.db.upsert_mobility_data(**upsert_obj)
