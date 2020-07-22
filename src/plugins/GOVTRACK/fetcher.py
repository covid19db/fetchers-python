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

import json
import logging
import requests
import pandas as pd
import numpy as np

from utils.fetcher.base_government_response import BaseGovernmentResponseFetcher
from .utils import parser, to_int
from datetime import datetime, timedelta

__all__ = ('StringencyFetcher',)

logger = logging.getLogger(__name__)


class StringencyFetcher(BaseGovernmentResponseFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'GOVTRACK'

    def fetch(self):
        # First intensive care hospitalisation on 2020-01-01
        if self.sliding_window_days:
            date_from = (datetime.now() - timedelta(days=self.sliding_window_days)).strftime('%Y-%m-%d')
        else:
            date_from = '2020-01-01'

        date_to = datetime.today().strftime('%Y-%m-%d')
        api_data = requests.get(
            f'https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/date-range/{date_from}/{date_to}').json()
        return parser(api_data, self.country_codes_translator)

    def fetch_details(self, country_code, date_value):
        api_details = requests.get(
            f'https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/actions/{country_code}/{date_value}'
        ).json()
        if "stringencyData" in api_details:
            api_details.pop("stringencyData")
        return api_details

    def fetch_csv(self):
        df = pd.read_csv(
            f'https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv')
        df = df.replace({np.nan: None})
        df = df.merge(self.country_codes_translator.translation_pd, right_on='Alpha-3 code', left_on='CountryCode',
                      how='left')
        return df

    def run(self):
        # RAW Govtrack data
        raw_govtrack_data = self.fetch_csv()
        for index, record in raw_govtrack_data.iterrows():
            if pd.isna(record['English short name lower case']):
                logger.error(f"Unable to decode: {record['CountryCode']} -> {record['CountryName']} ")

            upsert_obj = {
                'source': self.SOURCE,
                'gid': record['CountryCode'],
                'country': record['English short name lower case'],
                'countrycode': record['CountryCode'],
                'date': pd.to_datetime(record['Date'], format='%Y%m%d').strftime('%Y-%m-%d'),
                'c1_school_closing': to_int(record['C1_School closing']),
                'c1_flag': to_int(record['C1_Flag']),
                'c2_workplace_closing': to_int(record['C2_Workplace closing']),
                'c2_flag': to_int(record['C2_Flag']),
                'c3_cancel_public_events': to_int(record['C3_Cancel public events']),
                'c3_flag': to_int(record['C3_Flag']),
                'c4_restrictions_on_gatherings': to_int(record['C4_Restrictions on gatherings']),
                'c4_flag': to_int(record['C4_Flag']),
                'c5_close_public_transport': to_int(record['C5_Close public transport']),
                'c5_flag': to_int(record['C5_Flag']),
                'c6_stay_at_home_requirements': to_int(record['C6_Stay at home requirements']),
                'c6_flag': to_int(record['C6_Flag']),
                'c7_restrictions_on_internal_movement': to_int(record['C7_Restrictions on internal movement']),
                'c7_flag': to_int(record['C7_Flag']),
                'c8_international_travel_controls': to_int(record['C8_International travel controls']),
                'e1_income_support': to_int(record['E1_Income support']),
                'e1_flag': to_int(record['E1_Flag']),
                'e2_debtcontract_relief': to_int(record['E2_Debt/contract relief']),
                'e3_fiscal_measures': record['E3_Fiscal measures'],
                'e4_international_support': record['E4_International support'],
                'h1_public_information_campaigns': to_int(record['H1_Public information campaigns']),
                'h1_flag': to_int(record['H1_Flag']),
                'h2_testing_policy': to_int(record['H2_Testing policy']),
                'h3_contact_tracing': to_int(record['H3_Contact tracing']),
                'h4_emergency_investment_in_healthcare': record['H4_Emergency investment in healthcare'],
                'h5_investment_in_vaccines': record['H5_Investment in vaccines'],
                'm1_wildcard': record['M1_Wildcard'],
                'stringency_index': record['StringencyIndex'],
                'stringency_indexfordisplay': record['StringencyIndexForDisplay'],
                'stringency_legacy_index': record['StringencyLegacyIndex'],
                'stringency_legacy_indexfordisplay': record['StringencyLegacyIndexForDisplay'],
                'government_response_index': record['GovernmentResponseIndex'],
                'government_response_index_for_display': record['GovernmentResponseIndexForDisplay'],
                'containment_health_index': record['ContainmentHealthIndex'],
                'containment_health_index_for_display': record['ContainmentHealthIndexForDisplay'],
                'economic_support_index': record['EconomicSupportIndex'],
                'economic_support_index_for_display': record['EconomicSupportIndexForDisplay']
            }
            self.upsert_data(**upsert_obj)

        # GOVTRACK data from API
        govtrack_data = self.fetch()
        for index, record in govtrack_data.iterrows():
            govtrack_actions = self.fetch_details(record['country_code'], record['date_value'])
            upsert_obj = {
                'source': self.SOURCE,
                'date': record['date_value'],
                'country': record['English short name lower case'],
                'countrycode': record['country_code'],
                'gid': [record['country_code']],
                'actions': json.dumps(govtrack_actions)
            }
            self.upsert_data(**upsert_obj)
