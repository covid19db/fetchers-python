import logging
import requests
from utils.fetcher_abstract import AbstractFetcher
from datetime import date

__all__ = ('NigeriaCDC',)

logger = logging.getLogger(__name__)


class NigeriaCDC(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'NGA_CDC'

    def fetch(self):
        url = 'https://services5.arcgis.com/Y2O5QPjedp8vHACU/arcgis/rest/services/NgeriaCovid19/FeatureServer/0/query?f=json&where=ConfCases%20%3E%3D%200&returnGeometry=false&returnSpatialRel=false&outFields=*'
        data = requests.get(url).json()
        return data["features"]

    def run(self):
        logger.info("Processing number of cases in Nigeria by province")

        data = self.fetch()
        date_ = date.today().strftime('%Y-%m-%d')

        for record in data:
            details = record.get('attributes')
            state = details.get('NAME_1')
            confirmed = int(details.get('ConfCases'))
            recovered = int(details.get('Recovery'))
            dead = int(details.get('Deaths'))

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=state,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True
            )

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                # Pulling directly from Nigeria Centre for Disease Control, https://covid19.ncdc.gov.ng/
                'source': self.SOURCE,
                'date': date_,
                'country': 'Nigeria',
                'countrycode': 'NGA',
                'adm_area_1': adm_area_1,
                'adm_area_2': None,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': dead,
                'recovered': recovered,
                'gid': gid
            }

            self.db.upsert_epidemiology_data(**upsert_obj)
