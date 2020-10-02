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
from uk_covid19 import Cov19API

__all__ = ('EnglandFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class EnglandFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data from Public Health England'''
    LOAD_PLUGIN = True
    SOURCE = 'GBR_PHE'  # Public Health England
    START_DATE = '2020-02-28'

    def fetch_uk(self):
        uk = ['areaType=overview', 'date>' + self.get_first_date_to_fetch(self.START_DATE)]

        cases_and_deaths = {
            "date": "date",
            "areaName": "areaName",
            "areaType": "areaType",
            "cases": "cumCasesByPublishDate",
            "tests": "cumTestsByPublishDate",
            "deaths": "cumDeaths28DaysByDeathDate",
            "cumAdmissions": "cumAdmissions"
        }

        api = Cov19API(filters=uk, structure=cases_and_deaths)
        data = api.get_json()
        return data

    def fetch_nation(self):
        nation = ['areaType=nation', 'areaName=England', 'date>' + self.get_first_date_to_fetch(self.START_DATE)]

        cases_and_deaths = {
            "date": "date",
            "areaName": "areaName",
            "areaType": "areaType",
            "cases": "cumCasesByPublishDate",
            "tests": "cumTestsByPublishDate",
            "deaths": "cumDeaths28DaysByDeathDate",
            "cumAdmissions": "cumAdmissions"
        }

        api = Cov19API(filters=nation, structure=cases_and_deaths)
        data = api.get_json()
        return data

    def fetch_utla(self):
        utla = ['areaType=utla', 'date>' + self.get_first_date_to_fetch(self.START_DATE)]

        cases = {
            "date": "date",
            "areaName": "areaName",
            "areaType": "areaType",
            "areaCode": "areaCode",
            "cases": "cumCasesBySpecimenDate",
            "tests": "cumTestsByPublishDate",
            "cumAdmissions": "cumAdmissions"
        }

        api = Cov19API(filters=utla, structure=cases)
        data = api.get_json()
        return data

    def fetch_ltla(self):
        ltla = ['areaType=ltla', 'date>' + self.get_first_date_to_fetch(self.START_DATE)]

        cases = {
            "date": "date",
            "areaName": "areaName",
            "areaType": "areaType",
            "areaCode": "areaCode",
            "cases": "cumCasesBySpecimenDate",
            "tests": "cumTestsByPublishDate",
            "cumAdmissions": "cumAdmissions"
        }

        api = Cov19API(filters=ltla, structure=cases)
        data = api.get_json()
        return data

    def upsert_uk_data(self, data):
        for record in data:

            # only use English local authorities
            region_type = record.get('areaType')
            if region_type in ['utla', 'ltla'] and record.get('areaCode')[0] != 'E':
                continue

            region = record.get('areaName')
            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=region,
                input_adm_area_2=region_type,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': str(record.get('date')),
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid,
                'dead': record.get('deaths'),
                'confirmed': record.get('cases'),
                'tested': record.get('tests'),
                'hospitalised': record.get('cumAdmissions')
            }

            self.upsert_data(**upsert_obj)

    def run(self):

        methods = [self.fetch_uk, self.fetch_nation, self.fetch_utla, self.fetch_ltla]
        for method in methods:
            data = method()['data']
            self.upsert_uk_data(data)
