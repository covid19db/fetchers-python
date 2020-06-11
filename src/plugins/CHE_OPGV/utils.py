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


def parser(data):
    """
    DESCRIPTION:
    A function to parse all data downloaded from CSV into the DB format.
    :param data: [pandas DataFrame] non-parsed data.
    :return: [pandas DataFrame] parsed data.
    """

    data = data.rename(columns={"abbreviation_canton_and_fl": "adm_area_1", "date": "date", "ncumul_tested": "tested",
                                "ncumul_conf": "confirmed", "ncumul_ICU": "hospitalised_icu",
                                "ncumul_hosp": "hospitalised", "ncumul_deceased": "dead"})

    data = data[['date', 'adm_area_1', 'tested', 'confirmed', 'dead', 'hospitalised', 'hospitalised_icu']]

    data = data.where(pd.notnull(data), '')

    return data
