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
from pandas import DataFrame

from utils.config import config
from datetime import timedelta
from .mapping import RegionMapping
from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

__all__ = ('PolandGovFetcher',)

from .utils import get_daily_report, get_regional_report_urls, get_recent_regional_report_url

logger = logging.getLogger(__name__)


class PolandGovFetcher(BaseEpidemiologyFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'POL_GOV'

    def update_cases(self, date: str, df: DataFrame, data_type: str, region_mapping: RegionMapping):
        duplicate_gids = []
        non_existing_gids = []
        translation = []
        for index, row in df.iterrows():
            # if not row['Powiat/Miasto']:
            #     continue
            #
            # adm_area_1, adm_area_2, adm_area_3, gid = region_mapping.find_nearest_translation(
            #     region_name=row['Powiat/Miasto'], adm_area_1=row['Województwo'])

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=row['Województwo'] if 'Województwo' in row.index else '',
                input_adm_area_2=row['Powiat/Miasto'] if 'Powiat/Miasto' in row.index else None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            # adm_area_1, adm_area_2, adm_area_3, gid = region_mapping.find_nearest_translation(
            #     region_name='', adm_area_1=row['Województwo'])
            print(
                f"{date} - {row['Województwo']}, '' -> {adm_area_1}, {adm_area_2}, {adm_area_3}, {gid}")

            translation.append({
                'region_name': row.get('Powiat/Miasto', ''),
                'area_name': row['Województwo'],
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid[0]
            })

            if gid[0] in duplicate_gids:
                print('GID ALREADY EXISTS!!!')
                duplicate_gids.append(gid[0])

            upsert_obj = {
                'source': self.SOURCE,
                'date': date.strftime('%Y-%m-%d'),
                'country': 'Poland',
                'countrycode': 'POL',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'gid': gid
            }

            # prv_day = self.get_data(source='POL_ROG', gid=gid, date=(date - timedelta(days=1)).strftime('%Y-%m-%d'))
            # if not prv_day:
            #     print(f"Unable to found data for: {adm_area_1}, {adm_area_2}, {adm_area_3}, {gid}")
            #     non_existing_gids.append(gid)
            #     continue
            #
            # if data_type == 'confirmed':
            #     cumulative_confirmed = prv_day['confirmed'] + row[df.columns[2]]
            #     upsert_obj['confirmed'] = row[df.columns[2]]
            # elif data_type == 'deaths':
            #     cumulative_dead = prv_day['dead'] + row[df.columns[2]]
            #     upsert_obj['dead'] = row[df.columns[2]]
            # else:
            #     raise Exception('Data type not supported!')
            #
            continue

            self.upsert_data(**upsert_obj)
        print(f'Non existing gids: {non_existing_gids}')
        print(f'Duplicates: {duplicate_gids}')

        import csv

        with open('translation_woj.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([
                                "countrycode, input_adm_area_1, input_adm_area_2, input_adm_area_3, adm_area_1, adm_area_2, adm_area_3, gid"])
            for value in translation:
                writer.writerow([
                    'POL', value['area_name'], value['region_name'], '',
                    value['adm_area_1'], value['adm_area_2'], value['adm_area_3'],
                    value['gid']]
                )

        print('---')

    def run(self):
        region_mapping = RegionMapping(self.data_adapter.conn)

        # Wojewodztwa
        report_urls = get_regional_report_urls("https://www.gov.pl/web/koronawirus/pliki-archiwalne-wojewodztwa")
        report_urls.append(
            get_recent_regional_report_url("https://www.gov.pl/web/koronawirus/wykaz-zarazen-koronawirusem-sars-cov-2"))
        for url in report_urls:
            df_data, date = get_daily_report(url)

            self.update_cases(date,
                              df_data[['Województwo', 'Liczba']],
                              'confirmed',
                              region_mapping)

        # Powiaty
        report_urls = get_regional_report_urls("https://www.gov.pl/web/koronawirus/pliki-archiwalne-powiaty")
        report_urls.append(get_recent_regional_report_url(
            "https://www.gov.pl/web/koronawirus/mapa-zarazen-koronawirusem-sars-cov-2-powiaty"))
        for url in report_urls:
            df_data, date = get_daily_report(url)

            # self.update_cases(date,
            #                   df_data[['Województwo', 'Powiat/Miasto', 'Liczba']],
            #                   'confirmed',
            #                   region_mapping)
            #
            # self.update_cases(date,
            #                   df_data[['Województwo', 'Powiat/Miasto', 'Wszystkie przypadki śmiertelne']],
            #                   'deaths',
            #                   region_mapping)
