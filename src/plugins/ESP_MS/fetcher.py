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

#
# Data from Ministerio de Sanidad (Gobierno de España)
# https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov-China/situacionActual.htm
#

from datetime import datetime, date
import logging
import time
import unicodedata
import pandas as pd
import requests
from tika import parser
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher
from .utils import get_ccaa_tables, get_fecha

__all__ = ('SpainMSFetcher',)

logger = logging.getLogger(__name__)


class SpainMSFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'ESP_MS'

    def fetch(self, no):
        logger.debug(f'Fetching Actualización nº {no}')
        r = requests.get(f'https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/'
                         f'alertasActual/nCov-China/documentos/Actualizacion_{no}_COVID-19.pdf')
        return parser.from_buffer(r.content) if r.status_code == 200 else None

    def run(self):
        # ESP_MSVP stopped at Actualización nº 116 on 25.05.2020
        start = 116
        stop = (date.today() - date(2020, 5, 25)).days + 117
        if self.sliding_window_days:
            start = max(start, stop - self.sliding_window_days)

        for actualizacion in range(start, stop):
            parsed = self.fetch(actualizacion)
            time.sleep(5)  # crawl delay
            if parsed is None:
                continue
            content = unicodedata.normalize('NFKC', parsed['content'])
            fecha = datetime.strptime(get_fecha(content), '%d.%m.%Y').strftime('%Y-%m-%d')
            tabs = get_ccaa_tables(content, ['Tabla 1. Casos', 'Tabla 2. Casos'])

            if 'Acrobat Distiller' in parsed['metadata']['producer']:  # fragile
                tabs[0] = [[col for col in row if col != ''] for row in tabs[0]]
                tabs[1] = [[col for col in row if col != ''] for row in tabs[1]]

            df1 = pd.DataFrame([row[0:2] for row in tabs[0]], columns=['ccaa', 'confirmed'])
            df2 = pd.DataFrame([[row[i] for i in (0, 1, 3, 5)] for row in tabs[1]],
                               columns=['ccaa', 'hospitalised', 'hospitalised_icu', 'dead'])
            data = pd.merge(df1, df2, on='ccaa')

            for index, record in data.iterrows():
                # ccaa,confirmed,hospitalised,hospitalised_icu,dead
                ccaa = record[0]
                confirmed = int(record[1]) if pd.notna(record[1]) else None
                hospitalised = int(record[2]) if pd.notna(record[2]) else None
                hospitalised_icu = int(record[3]) if pd.notna(record[3]) else None
                dead = int(record[4]) if pd.notna(record[4]) else None

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    country_code='ESP',
                    input_adm_area_1=ccaa,
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
                    'confirmed': confirmed,
                    'dead': dead,
                    'hospitalised': hospitalised,
                    'hospitalised_icu': hospitalised_icu,
                    'gid': gid
                }
                self.upsert_data(**upsert_obj)
