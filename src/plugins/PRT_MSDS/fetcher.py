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

from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime
import logging
import pandas as pd
import numpy as np

__all__ = ('PRT_MSDSFetcher',)

""" 
    site-location: https://github.com/dssg-pt/covid19pt-data
    
    COVID19-Portugal Data for Portugal created, maintained and hosted by Data Science for Social Good Portugal
.
    
    The data source: Portuguese Ministry of Health.
    
"""
logger = logging.getLogger(__name__)


class PRT_MSDSFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'PRT_MSDS'

    def country_fetch(self):

        """
                        This url mainly provide detailed data of Portugal Covid19 including regional data.
        """

        url = 'https://raw.githubusercontent.com/dssg-pt/covid19pt-data/master/data.csv'
        logger.debug('Fetching Portugal country&province-level data from PRT_MSDS')

        return pd.read_csv(url)

    def run(self):

        """
                         This run functions mainly created country-level confirmed&dead&confirmed&hospitalised&icu
                         daily cumulative data and province-level cumulative confirmed&dead collection;
        
        """

        country_data = self.country_fetch()

        ### Get dates list
        date_list = list(country_data.data)

        ### For country-level data, we collected corresponding lists
        confirmed_list = np.array(country_data.confirmados, dtype='int')
        dead_list = np.array(country_data.obitos, dtype='int')
        recovered_list = np.array(country_data.recuperados, dtype='int')
        admitted_list = np.array(country_data.internados, dtype='int')
        icu_list = np.array(country_data.internados_uci, dtype='int')

        # Creating country-level data first
        for j in range(len(date_list)):
            ### Translating data format from DD-MM-YYYY to YYYY-MM-DD
            date = datetime.strptime(date_list[j], '%d-%m-%Y').strftime('%Y-%m-%d')

            confirmed = int(confirmed_list[j])
            dead = int(dead_list[j])
            recovered = int(recovered_list[j])
            admitted = int(admitted_list[j])
            if admitted < 0:
                admitted = None

            icu = int(icu_list[j])
            if icu < 0:
                icu = None

            upsert_obj = {
                # source is mandatory and is a code that identifies the  source
                'source': self.SOURCE,
                # date is also mandatory, the format must be YYYY-MM-DD
                'date': date,
                # country is mandatory and should be in English
                # the exception is "Ships"
                'country': "Portugal",
                # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                # an exception is ships, which has "---" as country code
                'countrycode': 'PRT',
                # adm_area_1, when available, is a wide-area administrative region, like a
                # Canadian province in this case. There are also subareas adm_area_2 and
                # adm_area_3
                'gid': ['PRT'],
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                # dead is the number of people who have died because of covid19, this is cumulative
                'dead': dead,
                'recovered': recovered,
                'hospitalised': admitted,
                'hospitalised_icu': icu

            }

            self.db.upsert_epidemiology_data(**upsert_obj)

        ### Manually get province names
        province_list = ['ARS Norte', 'ARS Centro', 'ARS Lisboa e Vale do Tejo', 'ARS Alentejo', \
                         'ARS Algarve', 'Região Autónoma dos Açores', 'Região Autónoma da Madeira', 'estrangeiro']

        ### Collect all confirmed data for each province
        confirmed_arsnorte_list = np.array(country_data.confirmados_arsnorte, dtype='int')
        confirmed_arscentro_list = np.array(country_data.confirmados_arscentro, dtype='int')
        confirmed_arslvt_list = np.array(country_data.confirmados_arslvt, dtype='int')
        confirmed_arsalentejo_list = np.array(country_data.confirmados_arsalentejo, dtype='int')
        confirmed_arsalgarve_list = np.array(country_data.confirmados_arsalgarve, dtype='int')
        confirmed_acores_list = np.array(country_data.confirmados_acores, dtype='int')
        confirmed_madeira_list = np.array(country_data.confirmados_madeira, dtype='int')
        confirmed_estrangeiro_list = np.array(country_data.confirmados_estrangeiro, dtype='int')

        ### Make a collection of confirmed collections for each province
        confirmed_collections = [confirmed_arsnorte_list, confirmed_arscentro_list, confirmed_arslvt_list, \
                                 confirmed_arsalentejo_list, confirmed_arsalgarve_list, confirmed_acores_list, \
                                 confirmed_madeira_list, confirmed_estrangeiro_list]

        ### Collect all dead data for each province
        dead_arsnorte_list = np.array(country_data.obitos_arsnorte, dtype='int')
        dead_arscentro_list = np.array(country_data.obitos_arscentro, dtype='int')
        dead_arslvt_list = np.array(country_data.obitos_arslvt, dtype='int')
        dead_arsalentejo_list = np.array(country_data.obitos_arsalentejo, dtype='int')
        dead_arsalgarve_list = np.array(country_data.obitos_arsalgarve, dtype='int')
        dead_acores_list = np.array(country_data.obitos_acores, dtype='int')
        dead_madeira_list = np.array(country_data.obitos_madeira, dtype='int')
        dead_estrangeiro_list = np.array(country_data.obitos_estrangeiro, dtype='int')

        ### Make a collection of dead collections for each province
        dead_collections = [dead_arsnorte_list, dead_arscentro_list, dead_arslvt_list, dead_arsalentejo_list, \
                            dead_arsalgarve_list, dead_acores_list, dead_madeira_list, dead_estrangeiro_list]

        ### For each date, we collect daily cumulative confirmed & dead numbers one by one province
        for k in range(len(date_list)):

            date = datetime.strptime(date_list[k], '%d-%m-%Y').strftime('%Y-%m-%d')

            for h in range(len(province_list)):

                province = province_list[h]

                confirmed = int(confirmed_collections[h][k])
                if confirmed < 0:
                    confirmed = None
                dead = int(dead_collections[h][k])
                if dead < 0:
                    dead = None

                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                    input_adm_area_1=province,
                    input_adm_area_2=None,
                    input_adm_area_3=None,
                    return_original_if_failure=True,
                    suppress_exception=True
                )
                upsert_obj = {
                    # source is mandatory and is a code that identifies the  source
                    'source': self.SOURCE,
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': "Portugal",
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'PRT',
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'gid': gid,
                    'confirmed': confirmed,
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': dead

                }

                self.db.upsert_epidemiology_data(**upsert_obj)
