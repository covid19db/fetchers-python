#
# Svenska Intensivvårdsregistret (SIR)
# https://portal.icuregswe.org/siri/report/corona.inrapp
#

from datetime import date, timedelta
import io
import logging
import time
import pandas as pd
import requests
from utils.fetcher_abstract import AbstractFetcher

__all__ = ('SwedenSIRFetcher',)

logger = logging.getLogger(__name__)


class SwedenSIRFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'SWE_SIR'

    def fetch(self, day):
        logger.debug(f'Getting ICU data for SWE for day {day}')
        url = 'https://portal.icuregswe.org/siri/api/reports/GenerateExcel'
        data = {
            'highChartUrl': '/api/reports/GenerateHighChart',
            'tableUrl': '/api/reports/GenerateExcel',
            'reportName': 'corona.inrapp',
            'startdat': day,
            'stopdat': day,
            'sasong[0]': 2019,
            'grouping': 'Region'
        }
        r = requests.post(url, data=data)
        return pd.read_excel(io.BytesIO(r.content), skiprows=[0])

    def run(self):
        today = date.today()
        for days in range(7):
            day = (today - timedelta(days=days)).isoformat()
            data = self.fetch(day)
            time.sleep(2)  # crawl delay

            for index, record in data.iterrows():
                # Region,Antal vårdtillfällen,Antal unika personer
                region = record[0]
                antal_unika_personer = int(record[2])

                upsert_obj = {
                    'source': self.SOURCE,
                    'date': day,
                    'country': 'Sweden',
                    'countrycode': 'SWE',
                    'hospitalised_icu': antal_unika_personer,
                    'gid': ['SWE']
                }

                # If not the whole kingdom
                if region != 'Hela riket':
                    success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                        country_code='SWE',
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
