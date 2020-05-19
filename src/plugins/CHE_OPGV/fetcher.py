import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from .utils import parser

__all__ = ('SwitzerlandFetcher',)

logger = logging.getLogger(__name__)


class SwitzerlandFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
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

    def fetch(self):
        # a csv file to be downloaded
        url = 'https://github.com/openZH/covid_19/raw/master/COVID19_Fallzahlen_CH_total.csv'
        return pd.read_csv(url)

    def run(self):
        data = self.fetch()
        data = data[data["abbreviation_canton_and_fl"]!="FL"]
        # Parse data into the scheme of our database
        data = parser(data)
        for index, record in data.iterrows():
            date = record[0]  # we expect date to be in YYYY-MM-DD format
            adm_area_1 = record[1]
            tested = int(record[2]) if record[2] != '' else None
            confirmed = int(record[3]) if record[3] != '' else None
            recovered = None #int(record[3]) if record[3] != '' else None   NOT PROVIDED IN THE CSV
            dead = int(record[4]) if record[4] != '' else None
            hospitalised = int(record[5]) if record[5] != '' else None
            hospitalised_icu = int(record[6]) if record[6] != '' else None

            upsert_obj = {
                'source': 'CHE_OPGOV',
                'date': date,
                'country': 'Switzerland',
                'countrycode': 'CHE',
                'adm_area_1': self.region_names['CH.'+adm_area_1],
                'gid': [self.region_GIDs['CH.'+adm_area_1]],
                'tested': tested,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,         #None NOT PROVIDED IN THE CSV
                'hospitalised': hospitalised,   
                'hospitalised_icu': hospitalised_icu
            }

            self.db.upsert_epidemiology_data(**upsert_obj)
