# Copyright University of Oxford 2020
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

import pandas as pd
from typing import Dict

from utils.country_codes_translator.translator import CountryCodesTranslator


def parser(api_data: Dict, country_codes_translator: CountryCodesTranslator):
    """
    DESCRIPTION:
    This function paste out daily updates of govtrack data from JSON into DataFrame format.
    :param api_data: [JSON] non-parsed data.
    :param country_codes_translator: [CountryCodesTranslator] country codes translator
    :return: [pandas DataFrame] parsed data.
    """
    records = []
    for item in api_data['data'].values():
        for record in item.values():
            records.append(record)

    govtrack_data = pd.DataFrame(records)
    govtrack_data.fillna(0, inplace=True)

    # Adding country name based on country code
    return govtrack_data.merge(country_codes_translator.translation_pd, right_on='Alpha-3 code', left_on='country_code',
                               how='left')


def to_int(data):
    return int(data) if pd.notna(data) else None
