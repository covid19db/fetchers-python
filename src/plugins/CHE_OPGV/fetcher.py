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
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher
from .utils import parser

__all__ = ('SwitzerlandFetcher',)

logger = logging.getLogger(__name__)


class SwitzerlandFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
<<<<<<< HEAD
    SOURCE = 'CHE_OPGOV'
    region_names = {'CH.AG': 'Aargau',
                    'CH.AR': 'Appenzell Ausserrhoden',
                    'CH.AI': 'Appenzell Innerrhoden',
                    'CH.BL': 'Basel-Landschaft',
                    'CH.BS': 'Basel-Stadt',
                    'CH.BE': 'Bern',
                    'CH.FR': 'Fribourg',
                    'CH.GE': 'Genève',
                    'CH.GL': 'Glarus',
                    'CH.GR': 'Graubünden',
                    'CH.JU': 'Jura',
                    'CH.LU': 'Lucerne',
                    'CH.NE': 'Neuchâtel',
                    'CH.NW': 'Nidwalden',
                    'CH.OW': 'Obwalden',
                    'CH.SG': 'Sankt Gallen',
                    'CH.SH': 'Schaffhausen',
                    'CH.SZ': 'Schwyz',
                    'CH.SO': 'Solothurn',
                    'CH.TG': 'Thurgau',
                    'CH.TI': 'Ticino',
                    'CH.UR': 'Uri',
                    'CH.VS': 'Valais',
                    'CH.VD': 'Vaud',
                    'CH.ZG': 'Zug',
                    'CH.ZH': 'Zürich'}
    region_GIDs = {'CH.AG': 'CHE.1_1',
                   'CH.AR': 'CHE.2_1',
                   'CH.AI': 'CHE.3_1',
                   'CH.BL': 'CHE.4_1',
                   'CH.BS': 'CHE.5_1',
                   'CH.BE': 'CHE.6_1',
                   'CH.FR': 'CHE.7_1',
                   'CH.GE': 'CHE.8_1',
                   'CH.GL': 'CHE.9_1',
                   'CH.GR': 'CHE.10_1',
                   'CH.JU': 'CHE.11_1',
                   'CH.LU': 'CHE.12_1',
                   'CH.NE': 'CHE.13_1',
                   'CH.NW': 'CHE.14_1',
                   'CH.OW': 'CHE.15_1',
                   'CH.SG': 'CHE.16_1',
                   'CH.SH': 'CHE.17_1',
                   'CH.SZ': 'CHE.18_1',
                   'CH.SO': 'CHE.19_1',
                   'CH.TG': 'CHE.20_1',
                   'CH.TI': 'CHE.21_1',
                   'CH.UR': 'CHE.22_1',
                   'CH.VS': 'CHE.23_1',
                   'CH.VD': 'CHE.24_1',
                   'CH.ZG': 'CHE.25_1',
                   'CH.ZH': 'CHE.26_1'}
=======
    region_names = {'CH.AG': 'Aargau',
  'CH.AR': 'Appenzell Ausserrhoden',
  'CH.AI': 'Appenzell Innerrhoden',
  'CH.BL': 'Basel-Landschaft',
  'CH.BS': 'Basel-Stadt',
  'CH.BE': 'Bern',
  'CH.FR': 'Fribourg',
  'CH.GE': 'Genève',
  'CH.GL': 'Glarus',
  'CH.GR': 'Graubünden',
  'CH.JU': 'Jura',
  'CH.LU': 'Lucerne',
  'CH.NE': 'Neuchâtel',
  'CH.NW': 'Nidwalden',
  'CH.OW': 'Obwalden',
  'CH.SG': 'Sankt Gallen',
  'CH.SH': 'Schaffhausen',
  'CH.SZ': 'Schwyz',
  'CH.SO': 'Solothurn',
  'CH.TG': 'Thurgau',
  'CH.TI': 'Ticino',
  'CH.UR': 'Uri',
  'CH.VS': 'Valais',
  'CH.VD': 'Vaud',
  'CH.ZG': 'Zug',
  'CH.ZH': 'Zürich'}
    region_GIDs = {'CH.AG': 'CHE.1_1',
  'CH.AR': 'CHE.2_1',
  'CH.AI': 'CHE.3_1',
  'CH.BL': 'CHE.4_1',
  'CH.BS': 'CHE.5_1',
  'CH.BE': 'CHE.6_1',
  'CH.FR': 'CHE.7_1',
  'CH.GE': 'CHE.8_1',
  'CH.GL': 'CHE.9_1',
  'CH.GR': 'CHE.10_1',
  'CH.JU': 'CHE.11_1',
  'CH.LU': 'CHE.12_1',
  'CH.NE': 'CHE.13_1',
  'CH.NW': 'CHE.14_1',
  'CH.OW': 'CHE.15_1',
  'CH.SG': 'CHE.16_1',
  'CH.SH': 'CHE.17_1',
  'CH.SZ': 'CHE.18_1',
  'CH.SO': 'CHE.19_1',
  'CH.TG': 'CHE.20_1',
  'CH.TI': 'CHE.21_1',
  'CH.UR': 'CHE.22_1',
  'CH.VS': 'CHE.23_1',
  'CH.VD': 'CHE.24_1',
  'CH.ZG': 'CHE.25_1',
  'CH.ZH': 'CHE.26_1'}
>>>>>>> 766215f30d4ee0bd98ea7897f06343db1889ddbb

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://github.com/openZH/covid_19/raw/master/COVID19_Fallzahlen_CH_total.csv'
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()
<<<<<<< HEAD
        data = data[data["abbreviation_canton_and_fl"] != "FL"]
=======
        data = data[data["abbreviation_canton_and_fl"]!="FL"]
>>>>>>> 766215f30d4ee0bd98ea7897f06343db1889ddbb
        # Parse data into the scheme of our database
        data = parser(data)
        for index, record in data.iterrows():
            date = record[0]  # we expect date to be in YYYY-MM-DD format
            adm_area_1 = record[1]
            tested = int(record[2]) if record[2] != '' else None
            confirmed = int(record[3]) if record[3] != '' else None
            recovered = None  # int(record[3]) if record[3] != '' else None   NOT PROVIDED IN THE CSV
            dead = int(record[4]) if record[4] != '' else None
            hospitalised = int(record[5]) if record[5] != '' else None
            hospitalised_icu = int(record[6]) if record[6] != '' else None

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'Switzerland',
                'countrycode': 'CHE',
<<<<<<< HEAD
                'adm_area_1': self.region_names['CH.' + adm_area_1],
                'gid': [self.region_GIDs['CH.' + adm_area_1]],
=======
                'adm_area_1': self.region_names['CH.'+adm_area_1],
                'gid': [self.region_GIDs['CH.'+adm_area_1]],
>>>>>>> 766215f30d4ee0bd98ea7897f06343db1889ddbb
                'tested': tested,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,  # None NOT PROVIDED IN THE CSV
                'hospitalised': hospitalised,
                'hospitalised_icu': hospitalised_icu
            }

            self.upsert_data(**upsert_obj)
