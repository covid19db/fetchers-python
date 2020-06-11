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

#
# France data collected Cedric Guadalupe from Santé Publique France
# https://github.com/cedricguadalupe/FRANCE-COVID-19
#

from datetime import datetime
import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('FranceSPFCGFetcher',)

logger = logging.getLogger(__name__)


class FranceSPFCGFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'FRA_SPFCG'

    def fetch(self, category):
        logger.debug(f'Going to fetch France {category}')
        data = pd.read_csv(f'https://raw.githubusercontent.com/cedricguadalupe/FRANCE-COVID-19/'
                           f'master/france_coronavirus_time_series-{category}.csv',
                           index_col=[0])
        key = category if category != 'deaths' else 'dead'

        for index, row in data.iterrows():
            # Date,Auvergne-Rhône-Alpes,Bourgogne-Franche-Comté,Bretagne,Centre-Val de Loire,
            # Corse,Grand Est,Hauts-de-France,Île-de-France,Normandie,Nouvelle-Aquitaine,
            # Occitanie,Pays de la Loire,Provence-Alpes-Côte d'Azur,Guadeloupe,Saint-Barthélémy,
            # Saint-Martin,Martinique,Guyane,La Réunion,Mayotte,Nouvelle-Calédonie,
            # Total
            date = datetime.strptime(index, '%d/%m/%Y').strftime('%Y-%m-%d')

            for region, record in row.items():
                if pd.notna(record):
                    upsert_obj = {
                        'source': self.SOURCE,
                        'date': date,
                        'country': 'France',
                        'countrycode': 'FRA',
                        key: int(record),
                        'gid': ['FRA']
                    }

                    if region != 'Total':
                        success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                            country_code='FRA',
                            input_adm_area_1=region,
                            input_adm_area_2=None,
                            input_adm_area_3=None,
                            return_original_if_failure=True
                        )
                        upsert_obj['adm_area_1'] = adm_area_1
                        upsert_obj['adm_area_2'] = adm_area_2
                        upsert_obj['adm_area_3'] = adm_area_3
                        upsert_obj['gid'] = gid

                    self.db.upsert_epidemiology_data(**upsert_obj)

    def run(self):
        self.fetch('confirmed')
        self.fetch('deaths')
        self.fetch('recovered')
