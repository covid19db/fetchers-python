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
import pandas as pd
import os
import sys
import copy
import numpy as np
from datetime import datetime

__all__ = ('CHN_ICL_Fetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)

"""
    site-location: https://github.com/mrc-ide/covid19_mainland_China_report
    COVID19 data for China created and maintained by mrc-ide

    Data originally from the lists in the following link

    https://github.com/mrc-ide/covid19_mainland_China_report#websites-of-health-commissions-in-mainland-china



"""


class CHN_ICL_Fetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'CHN_ICL'

    def fetch(self, name):
        url = "https://github.com/mrc-ide/covid19_mainland_China_report/blob/master/Data/extracted_epidemic_trend.xlsx?raw=true"
        return pd.read_excel(url, sheet_name=name)

    # Get the last date indices if more than one cells contain the same date
    def get_last_indices(self, date_list):

        reverse_list = copy.deepcopy(date_list)
        reverse_list.reverse()
        reverse_unique_indices = list(set([reverse_list.index(reverse_list[i]) for i in range(len(reverse_list))]))

        reverse_unique_indices.reverse()

        return int(len(date_list) - 1) - np.array(reverse_unique_indices)

    def replace_datastart(self, date_list, date_substitute_list):
        data_final_list = []
        for i in range(len(date_list)):
            date_ = date_list[i]
            if isinstance(date_, datetime):
                data_final_list.append(date_)
            else:
                data_final_list.append(date_substitute_list[i])

        return data_final_list

    def CHN_fetcher(self, province_name):

        logger.info("Processing number of cases in " + province_name)

        df = self.fetch(province_name)
        date_list = list(df["Unnamed: 3"][1:])
        date_substitute_list = list(df["Unnamed: 5"][1:])
        confirmed_list = np.array(df["Cumulative number of cases"][1:])
        recovered_list = np.array(df["Unnamed: 9"][1:])
        dead_list = np.array(df["Unnamed: 10"][1:])

        # If data being reported twice at the same day, we get the last indice of the date because this data is cumulating
        # indices = self.get_last_indices(date_list)
        indices = self.get_last_indices(self.replace_datastart(date_list, date_substitute_list))

        for i in range(len(indices)):

            j = indices[i]
            date_ = date_list[j]

            if isinstance(date_, datetime):
                date = date_.strftime('%Y-%m-%d')
            else:
                logger.warning(f"Wrong date format: {date_}, type: {type(date_)}, skipping line ")
                continue

            confirmed = confirmed_list[j] if pd.notna(confirmed_list[j]) else None
            recovered = recovered_list[j] if pd.notna(recovered_list[j]) else None
            dead = dead_list[j] if pd.notna(dead_list[j]) else None

            if province_name == "National (mainland)":
                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'China',
                    'countrycode': 'CHN',
                    'adm_area_1': None,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'gid': ['CHN'],
                    'confirmed': confirmed,
                    'recovered': recovered,
                    'dead': dead
                }
            else:
                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=province_name,
                    input_adm_area_2=None,
                    return_original_if_failure=True,
                    suppress_exception=True
                )

                # add the epidemiological properties to the object if they exist
                upsert_obj = {
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'China',
                    'countrycode': 'CHN',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'gid': gid,
                    'confirmed': confirmed,
                    'recovered': recovered,
                    'dead': dead
                }

            self.upsert_data(**upsert_obj)

    def run(self):

        sheet_names = ["National (mainland)", "Wuhan", "Hubei", "Guangdong", "Zhejiang",
                       "Henan", "Hunan", "Anhui", "Shandong", "Beijing",
                       "Sichuan", "Chongqing", "Fujian", "Jiangxi", "Jiangsu",
                       "Shanghai", "Guangxi", "Shaanxi", "Yunnan", "Liaoning",
                       "Hebei", "Heilongjiang", "Tianjin", "Hainan", "Shanxi",
                       "Gansu", "Inner Mongolia", "Ningxia", "Guizhou", "Jilin",
                       "Xinjiang", "Qinghai", "Tibet"]

        self.CHN_fetcher("National (mainland)")

        # Wuhan is excluded as it is the only adm_area_2 data
        for province_name in sheet_names[2:]:
            self.CHN_fetcher(province_name)
