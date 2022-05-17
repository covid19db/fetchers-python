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

    def fetch_county_names(self):
        # collects first 2000 rows - all that can be transferred - and then uses it to get county names
        url = 'https://services1.arcgis.com/eNO7HHeQ3rUcBllm/arcgis/rest/services/Covid19CountyStatisticsHPSCIrelandPointData/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson'
        data = requests.get(url).json()["features"]

        unique_counties = []
        for record in data:
            county_name = record.get('properties').get('CountyName')
            if county_name not in unique_counties:
                unique_counties.append(county_name);

        return unique_counties

    def fetch(self, county):
        # Health Surveillance Protection Centre
        # https://covid19ireland-geohive.hub.arcgis.com/

        url = f'https://services1.arcgis.com/eNO7HHeQ3rUcBllm/arcgis/rest/services/Covid19CountyStatisticsHPSCIrelandPointData/FeatureServer/0/query?where=CountyName%20%3D%20%27{county}%27&outFields=*&outSR=4326&f=json'
        data = requests.get(url).json()
        return data["features"]

    def run(self):
        logger.info("Processing number of cases in Ireland by county")

        # Get list of all counties
        county_names = self.fetch_county_names()

        for county in county_names:

            json_object = self.fetch(county)
            print(county)
            print(len(json_object))

            for record in json_object:
                data = record.get('attributes')
                confirmed = data.get('ConfirmedCovidCases') or 0
                county = data.get('CountyName')
                timestamp = data.get('TimeStampDate') / 1000
                date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(input_adm_area_1=county,
                                                                                          input_adm_area_2=None,
                                                                                          input_adm_area_3=None,
                                                                                          return_original_if_failure=True)

                # we need to build an object containing the data we want to add or update
                upsert_obj = {'source': self.SOURCE, 'date': date, 'country': 'Ireland', 'countrycode': 'IRL',
                              'adm_area_1': adm_area_1, 'adm_area_2': None, 'adm_area_3': None, 'confirmed': confirmed,
                              'gid': gid}

                self.upsert_data(**upsert_obj)
