from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime
import logging
import pandas as pd
import numpy as np

__all__ = ('BEL_ZHFetcher',)

""" 
    site-location: https://github.com/eschnou/covid19-be/blob/master/covid19-belgium.csv
    
    COVID19-Belgium Data for Belgium created, maintained and hosted by github: eschnou.
    
    The data sources include: 
        
        https://covid-19.sciensano.be/nl/covid-19-epidemiologische-situatie
        https://www.info-coronavirus.be/fr/news/
        https://news.belgium.be/en/corona
    
"""
logger = logging.getLogger(__name__)


class BEL_ZHFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def country_fetch(self):

        """
                        This url mainly provide cumulative data on the country-level.
        """

        url = 'https://raw.githubusercontent.com/eschnou/covid19-be/master/covid19-belgium.csv'
        logger.debug('Fetching Belgium country-level data from BEL_ZH')
        return pd.read_csv(url)


    def run(self):

        """
                        This run function mainly created country-level cumulative tested&confirmed&dead&hospitalised&ICU
                        collection from country_fetch;
        
        """

        country_data = self.country_fetch()

        ### Get dates & tested & confirmed & dead & hospitalised_icu & recovered lists
        time_list=list(country_data.date)
        tested_list=np.asarray(country_data.cumul_tests,dtype='int')
        confirmed_list=np.asarray(country_data.cumul_cases,dtype='int')
        hospitalized_list=np.asarray(country_data.hospitalized,dtype='int')
        icu_list=np.asarray(country_data.icu,dtype='int')
        dead_list=np.asarray(country_data.cumul_deceased,dtype='int')
        recovered_list=np.asarray(country_data.cumul_released,dtype='int')

        
        for k in range(len(time_list)):
            
            ### Translating data format from DD/MM/YYYY to YYYY-MM-DD
            date_ddmmyy = time_list[k]
            date = datetime.strptime(date_ddmmyy, '%d/%m/%Y').strftime('%Y-%m-%d')
    
            tested=tested_list[k]
            if tested<0:
                tested=None
            else:
                tested=int(tested)
                
            confirmed=confirmed_list[k]
            if confirmed<0:
                confirmed=None
            else:
                confirmed=int(confirmed)
                
            dead=dead_list[k]
            if dead<0:
                dead=None
            else:
                dead=int(dead)
                
            HOSPITALISED=hospitalized_list[k]
            if HOSPITALISED<0:
                HOSPITALISED=None 
            else:
                HOSPITALISED=int(HOSPITALISED)
                
            hospitalised_icu=icu_list[k]
            if hospitalised_icu<0:
                hospitalised_icu=None
            else:
                hospitalised_icu=int(hospitalised_icu)
                
            recovered=recovered_list[k]  
            if recovered<0:
                recovered=None            
            else:
                recovered=int(recovered)


            upsert_obj = {
                    # source is mandatory and is a code that identifies the  source
                    'source': 'BEL_ZH',
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': "Belgium",
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'BEL',
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    'adm_area_1': None,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'tested':    tested,
                    'confirmed': confirmed,
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': dead,
                    'hospitalised': HOSPITALISED,
                    'hospitalised_icu': hospitalised_icu,
                    'recovered': recovered

                }

            self.db.upsert_epidemiology_data(**upsert_obj)



