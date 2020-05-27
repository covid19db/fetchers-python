import logging
import pandas as pd
import json
import requests
from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime, date

__all__ = ('NigeriaSO',)

logger = logging.getLogger(__name__)


class NigeriaSO(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'NGA_SO'

    def fetch(self):
        url = 'https://covidnigeria.herokuapp.com/'
        data = requests.get(url).json()
        return data["data"]

    def run(self):
        logger.info("Processing total number of cases in Nigeria")

        data = self.fetch()

        date_ = date.today().strftime('%Y-%m-%d')
        tested = int(data["totalSamplesTested"])
        confirmed = int(data["totalConfirmedCases"])
        recovered = int(data["discharged"])
        dead = int(data["death"])

        # we need to build an object containing the data we want to add or update
        upsert_obj = {
            # source is https://github.com/sink-opuba/covid-19-nigeria-api
            # Pulls information from Nigeria Centre for Disease Control, https://covid19.ncdc.gov.ng/
            'source': self.SOURCE,
            'date': date_,
            'country': 'Nigeria',
            'countrycode': 'NGA',
            'adm_area_1': None,
            'adm_area_2': None,
            'adm_area_3': None,
            'tested': tested,
            'confirmed': confirmed,
            'dead': dead,
            'recovered': recovered,
            'gid': ['NGA']
        }

        self.db.upsert_epidemiology_data(**upsert_obj)
