#
# Fetcher of the regional statistics of Italy
# as compiled by Davide Magno from Protezione Civile on his Github repository
# see https://github.com/DavideMagno/ItalianCovidData
#

import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('ItalyPCDMFetcher',)

logger = logging.getLogger(__name__)


class ItalyPCDMFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch(self):
        return pd.read_csv('https://raw.githubusercontent.com/DavideMagno/ItalianCovidData/'
                           'master/Daily_Covis19_Italian_Data_Cumulative.csv')

    def run(self):
        data = self.fetch()

        for index, record in data.iterrows():
            # CSV columns: "Date","Region","Hospitalised","In ICU","Home Isolation",
            # "Healed","Dead","Tests"
            date = record[0]
            region = record[1]
            hospitalised = int(record[2])
            in_icu = int(record[3])
            quarantined = int(record[4])
            recovered = int(record[5])
            dead = int(record[6])
            tested = int(record[7])
            confirmed = in_icu + quarantined + recovered + dead

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='ITA',
                input_adm_area_1=region,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': 'ITA_PCDM',
                'date': date,
                'country': 'Italy',
                'countrycode': 'ITA',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'tested': tested,
                'confirmed': confirmed,
                'recovered': recovered,
                'dead': dead,
                'hospitalised': hospitalised,
                'hospitalised_icu': in_icu,
                'quarantined': quarantined,
                'gid': gid
            }
            self.db.upsert_epidemiology_data(**upsert_obj)
