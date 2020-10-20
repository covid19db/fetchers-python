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
from datetime import datetime
import requests

__all__ = ('EnglandMSOAFetcher',)

from utils.types import FetcherType
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class EnglandMSOAFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data from Public Health England'''
    TYPE = FetcherType.EPIDEMIOLOGY_MSOA
    LOAD_PLUGIN = True

    SOURCE = 'GBR_PHE'  # Public Health England
    START_DATE = '2020-02-28'

    def upsert_data(self, **kwargs):
        self.data_adapter.upsert_data(self.TYPE, **kwargs)

    def fetch_msoa(self):
        url = 'https://coronavirus.data.gov.uk/downloads/msoa_data/MSOAs_latest.json'
        data = requests.get(url).json()
        return data["data"]

    def week_parse(self, week_number):
        ''' take a week number and return Monday in form 2020-MM-DD'''
        # datetime does not follow ISO week number definitions, need offset
        week_number = week_number - 1
        d = '2020-' + str(week_number) + '-0'
        r = datetime.strptime(d, "%Y-%W-%w").strftime('%Y-%m-%d')
        return r

    def upsert_msoa_data(self):
        json_object = self.fetch_msoa()

        for record in json_object:
            adm_area_2 = record.get('utla19_nm')
            adm_area_3 = record.get('lad19_nm')
            msoa = record.get('msoa11_hclnm')
            msoa_code = record.get('msoa11_cd')
            case_data = record.get('msoa_data')
            for week in case_data:
                week_number = week.get('week')
                confirmed = week.get('value')
                date = self.week_parse(week_number)
                if confirmed == -99:
                    confirmed = None

                # we need to build an object containing the data we want to add or update
                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'United Kingdom',
                    'countrycode': 'GBR',
                    'adm_area_1': 'England',
                    'adm_area_2': adm_area_2,
                    'adm_area_3': adm_area_3,
                    'msoa': msoa,
                    'msoa_code': msoa_code,
                    'confirmed': confirmed
                }

                self.upsert_data(**upsert_obj)



    def run(self):

        self.upsert_msoa_data()
