import json
import logging
import requests
from .utils import parser
from datetime import datetime, timedelta

from utils.fetcher_abstract import AbstractFetcher, FetcherType

__all__ = ('StringencyFetcher',)

logger = logging.getLogger(__name__)


class StringencyFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    TYPE = FetcherType.GOVERNMENT_RESPONSE
    SOURCE = 'GOVTRACK'

    def fetch(self):
        # First intensive care hospitalisation on 2020-01-01
        if self.sliding_window_days:
            date_from = (datetime.now() - timedelta(days=self.sliding_window_days)).strftime('%Y-%m-%d')
        else:
            date_from = '2020-01-01'

        date_to = datetime.today().strftime('%Y-%m-%d')
        api_data = requests.get(
            f'https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/date-range/{date_from}/{date_to}').json()
        return parser(api_data, self.country_codes_translator)

    def fetch_details(self, country_code, date_value):
        api_details = requests.get(
            f'https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/actions/{country_code}/{date_value}'
        ).json()
        api_details.pop("stringencyData")
        return api_details

    def run(self):
        govtrack_data = self.fetch()
        for index, record in govtrack_data.iterrows():
            govtrack_actions = self.fetch_details(record['country_code'], record['date_value'])
            upsert_obj = {
                'source': self.SOURCE,
                'date': record['date_value'],
                'country': record['English short name lower case'],
                'countrycode': record['country_code'],
                'gid': [record['country_code']],
                'confirmed': int(record['confirmed']),
                'dead': int(record['deaths']),
                'stringency': int(record['stringency']),
                'stringency_actual': int(record['stringency_actual']),
                'actions': json.dumps(govtrack_actions)
            }
            self.db.upsert_government_response_data(**upsert_obj)
