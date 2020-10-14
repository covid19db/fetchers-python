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
from pandas import DataFrame

from utils.config import config
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher
from .utils import GoogleSpreadsheet, parse_date
from .mapping import RegionMapping

__all__ = ('PolandRogalskiFetcher',)

logger = logging.getLogger(__name__)


class PolandRogalskiFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'POL_ROG'

    def update_cases(self, df: DataFrame, data_type: str, region_mapping: RegionMapping):
        last_adm_area_1 = None
        for index, row in df.iterrows():
            if not row["Nazwa"]:
                continue

            adm_area_1, adm_area_2, adm_area_3, gid = region_mapping.find_nearest_translation(
                region_name=row["Nazwa"], adm_area_1=last_adm_area_1)
            last_adm_area_1 = adm_area_1

            column_index = 0
            for (col_name, col_data) in row.iteritems():
                column_index = column_index + 1
                if column_index < 3 or col_name in ['Kod', 'Nazwa']:  # Skip first 3 columns
                    continue

                if not col_name or not col_data or not col_data.replace(' ',''):
                    continue

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': parse_date(col_name),
                    'country': 'Poland',
                    'countrycode': 'POL',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': adm_area_2,
                    'adm_area_3': adm_area_3,
                    'gid': gid
                }

                if data_type == 'confirmed':
                    upsert_obj['confirmed'] = col_data
                elif data_type == 'deaths':
                    upsert_obj['dead'] = col_data
                else:
                    raise Exception('Data type not supported!')

                self.upsert_data(**upsert_obj)

    def run(self):
        # https://docs.google.com/spreadsheets/u/0/d/1Tv6jKMUYdK6ws6SxxAsHVxZbglZfisC8x_HZ1jacmBM
        SPREAD_SHEET_ID = '1Tv6jKMUYdK6ws6SxxAsHVxZbglZfisC8x_HZ1jacmBM'
        google_sheet = GoogleSpreadsheet(logger, config.GOOGLE_API_KEY)
        region_mapping = RegionMapping(self.data_adapter.conn)

        # Get confirmed cases
        df_confirmed = google_sheet.get_spreadsheet_data(SPREAD_SHEET_ID, 'Suma przypadków!A1:ZZZ')
        self.update_cases(df_confirmed, 'confirmed', region_mapping)

        # Get death cases
        df_deaths = google_sheet.get_spreadsheet_data(SPREAD_SHEET_ID, 'Suma zgonów!A1:ZZZ')
        self.update_cases(df_deaths, 'deaths', region_mapping)
