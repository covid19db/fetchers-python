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
import requests
from datetime import datetime

__all__ = ('IrelandHSPC',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class IrelandHSPC(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'IRL_HSPC'

    def fetch(self):
        # Health Surveillance Protection Centre
        # https://covid19ireland-geohive.hub.arcgis.com/search?groupIds=7e244cadac05461fb60b287a37b5ed2b

        url = 'https://opendata.arcgis.com/datasets/4779c505c43c40da9101ce53f34bb923_0.geojson'
        data = requests.get(url).json()
        return data["features"]

    def run(self):
        logger.info("Processing number of cases in Ireland by county")

        json_object = self.fetch()


        for record in json_object:
            data = record.get('properties')
            confirmed = data.get('ConfirmedCovidCases') or 0
            county = data.get('CountyName')
            timestamp = data.get('TimeStampDate')
            date = datetime.strptime(timestamp,'%Y/%m/%d %H:%M:%S+00').strftime('%Y-%m-%d')

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=county,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'Ireland',
                'countrycode': 'IRL',
                'adm_area_1': adm_area_1,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'gid': gid
            }

            self.upsert_data(**upsert_obj)
