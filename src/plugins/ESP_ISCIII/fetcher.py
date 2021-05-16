# Copyright (C) 2021 University of Oxford
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

#
# Data from Instituto de Salud Carlos III
# https://cnecovid.isciii.es/covid19/
#

import logging
import pandas as pd
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

__all__ = ('SpainISCIIIFetcher',)

logger = logging.getLogger(__name__)


class SpainISCIIIFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'ESP_ISCIII'

    def fetch(self):
        url = 'https://cnecovid.isciii.es/covid19/resources/casos_hosp_uci_def_sexo_edad_provres.csv'
        return pd.read_csv(url, na_filter=False) \
            .groupby(['provincia_iso', 'fecha']) \
            .sum() \
            .groupby(level=0) \
            .cumsum() \
            .reset_index()

    def run(self):
        logger.debug('Fetching provincial information')
        data = self.fetch()

        for index, record in data.iterrows():
            # provincia_iso,fecha,num_casos,num_hosp,num_uci,num_def
            provincia_iso = record[0]
            fecha = record[1]
            num_casos = int(record[2])
            num_hosp = int(record[3])
            num_uci = int(record[4])
            num_def = int(record[5])

            # Skip "no consta" provinces
            if provincia_iso == 'NC':
                continue

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='ESP',
                input_adm_area_1='ES-' + provincia_iso,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': fecha,
                'country': 'Spain',
                'countrycode': 'ESP',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'confirmed': num_casos,
                'dead': num_def,
                'hospitalised': num_hosp,
                'hospitalised_icu': num_uci,
                'gid': gid
            }
            self.upsert_data(**upsert_obj)
