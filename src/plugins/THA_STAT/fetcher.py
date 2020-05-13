#
# Covid-19 Infected Situation Reports
# https://covid19.th-stat.com/en/api
#

import logging
import requests
import pandas as pd
from datetime import datetime
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('ThailandSTATFetcher',)

logger = logging.getLogger(__name__)


class ThailandSTATFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def fetch_timeline(self):
        url = 'https://covid19.th-stat.com/api/open/timeline'
        return requests.get(url).json()

    def fetch_cases(self):
        url = 'https://covid19.th-stat.com/api/open/cases'
        return requests.get(url).json()

    def run(self):
        logger.debug('Fetching country-level information')
        data = self.fetch_timeline()

        for record in data['Data']:
            upsert_obj = {
                'source': 'THA_STAT',
                'date': datetime.strptime(record['Date'], '%m/%d/%Y') \
                                .strftime('%Y-%m-%d'),
                'country': 'Thailand',
                'countrycode': 'THA',
                'confirmed': int(record['Confirmed']),
                'dead': int(record['Deaths']),
                'recovered': int(record['Recovered']),
                'hospitalised': int(record['Hospitalized'])
            }
            self.db.upsert_epidemiology_data(**upsert_obj)

        logger.debug('Fetching regional information')
        data = self.fetch_cases()

        # Get cumulative counts from the cross table of dates and provinces
        df = pd.DataFrame(data['Data'], columns=['ConfirmDate', 'ProvinceEn'])
        crosstabsum = pd.crosstab(df.ConfirmDate.apply(lambda d: d[:10]),
                                  df.ProvinceEn) \
                        .sort_index() \
                        .cumsum()

        for confirmdate, row in crosstabsum.iterrows():
            for provinceen, confirmed in row.items():
                upsert_obj = {
                    'source': 'THA_STAT',
                    'date': confirmdate,
                    'country': 'Thailand',
                    'countrycode': 'THA',
                    'adm_area_1': provinceen,
                    'confirmed': int(confirmed)
                }
                self.db.upsert_epidemiology_data(**upsert_obj)
