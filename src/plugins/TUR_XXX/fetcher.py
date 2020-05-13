from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime
import logging
import pandas as pd
import numpy as np

__all__ = ('TUR_XXXFetcher',)

""" 
    site-location: https://github.com/ozanerturk/covid19-turkey-api
    
    COVID19-Turkey Data for Turkey created, maintained and hosted by ozanerturk.
    
    The data sources include: Turkish Ministry of Health (pulling every 5 mins).
    
"""
logger = logging.getLogger(__name__)


class TUR_XXXFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def country_fetch(self):

        """
                        This url mainly provide cumulative data on the country-level.
        """

        url = 'https://raw.githubusercontent.com/ozanerturk/covid19-turkey-api/master/dataset/timeline.csv'
        logger.debug('Fetching Turkey country-level data from TUR_XXX')
        return pd.read_csv(url)


    def run(self):

        """
                        This run function mainly created country-level cumulative tested&confirmed&dead&ICU&recovered
                        collection from country_fetch;
        
        """

        country_data = self.country_fetch()

        ### Get dates & tested & confirmed & dead & hospitalised_icu & recovered lists
        time_list = list(country_data.date)
        tested_list=np.asarray(country_data.totalTests)
        confirmed_list=np.asarray(country_data.totalCases)
        dead_list=np.asarray(country_data.totalDeaths)
        HOSPITALISED_ICU_list=np.asarray(country_data.totalIntensiveCare)
        recovered_list=np.asarray(country_data.totalRecovered)

        
        for k in range(len(time_list)):
            
            ### Translating data format from DD/MM/YYYY to YYYY-MM-DD
            date_ddmmyy = time_list[k]
            date = datetime.strptime(date_ddmmyy, '%d/%m/%Y').strftime('%Y-%m-%d')
    
            tested=tested_list[k]
            confirmed=confirmed_list[k]
            dead=dead_list[k]
            hospitalised_icu=HOSPITALISED_ICU_list[k]
            recovered=recovered_list[k]        

            upsert_obj = {
                    # source is mandatory and is a code that identifies the  source
                    'source': 'TUR_XXX',
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': "Turkey",
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'TUR',
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    'adm_area_1': None,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'tested':    int(tested),
                    'confirmed': int(confirmed),
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': int(dead),
                    'hospitalised_icu': int(hospitalised_icu),
                    'recovered': int(recovered)

                }

            self.db.upsert_epidemiology_data(**upsert_obj)


