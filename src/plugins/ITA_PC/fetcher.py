#
# Official data from Protezione Civile.
# https://github.com/pcm-dpc/COVID-19
#

import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('ItalyPCFetcher',)

logger = logging.getLogger(__name__)


class ItalyPCFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'ITA_PC'

    def fetch(self, category):
        return pd.read_csv(f'https://raw.githubusercontent.com/pcm-dpc/COVID-19/'
                           f'master/dati-{category}/dpc-covid19-ita-{category}.csv')

    def run(self):
        logger.debug('Going to fetch Protezione Civile data for regions')
        data = self.fetch('regioni')

        for index, record in data.iterrows():
            # data,stato,codice_regione,denominazione_regione,lat,long,ricoverati_con_sintomi,
            # terapia_intensiva,totale_ospedalizzati,isolamento_domiciliare,totale_positivi,
            # variazione_totale_positivi,nuovi_positivi,dimessi_guariti,deceduti,totale_casi,
            # tamponi,note_it,note_en
            # 2020-02-24T18:00:00,ITA,03,Lombardia,45.46679409,9.190347404,
            # 76,19,95,71,166,0,166,0,6,172,1463,,
            date = record[0][:10]  # 2020-02-24T18:00:00
            regione = record[3]
            inhospital_icu = int(record[7])
            inhospital = int(record[8])
            quarantined = int(record[9])
            confirmed = int(record[10])
            recovered = int(record[13])
            dead = int(record[14])
            tested = int(record[16])

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='ITA',
                input_adm_area_1=regione,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
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
                'hospitalised': inhospital,
                'hospitalised_icu': inhospital_icu,
                'quarantined': quarantined,
                'gid': gid
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug('Going to fetch Protezione Civile data for provinces')
        data = self.fetch('province')

        for index, record in data.iterrows():
            # data,stato,codice_regione,denominazione_regione,codice_provincia,
            # denominazione_provincia,sigla_provincia,lat,long,totale_casi,note_it,note_en
            date = record[0][:10]  # 2020-02-24T18:00:00
            regione = record[3]
            provincia = record[5]
            confirmed = int(record[9])

            # Skip if being defined/updated
            if provincia == 'In fase di definizione/aggiornamento':
                continue

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                country_code='ITA',
                input_adm_area_1=regione,
                input_adm_area_2=provincia,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'Italy',
                'countrycode': 'ITA',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': adm_area_3,
                'confirmed': confirmed,
                'gid': gid
            }
            self.db.upsert_epidemiology_data(**upsert_obj)
