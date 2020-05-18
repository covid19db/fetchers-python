from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime
import logging
import pandas as pd
import numpy as np

__all__ = ('BRA_MSHMFetcher',)

""" 
    site-location: https://github.com/elhenrico/covid19-Brazil-timeseries
    
    COVID19-Brazil Data for Brazil created, maintained and hosted by elhenrico.
    
    The data sources include: official communications of the Brazilian Ministry of Health.
    
"""
logger = logging.getLogger(__name__)


class BRA_MSHMFetcher(AbstractFetcher):
    LOAD_PLUGIN = True

    def province_confirmed_fetch(self):

        """
                        This url mainly provide cumulative data of confirmed cases on the province-level.
        """

        url = 'https://raw.githubusercontent.com/elhenrico/covid19-Brazil-timeseries/master/confirmed-cases.csv'
        logger.debug('Fetching Brazil province-level confirmed cases from BRA_MSHM')
        return pd.read_csv(url)

    def province_dead_fetch(self):

        """
                        This url mainly provide cumulative data of death data on the province-level.
        """

        url = 'https://raw.githubusercontent.com/elhenrico/covid19-Brazil-timeseries/master/deaths.csv'
        logger.debug('Fetching Brazil province-level death cases from BRA_MSHM')
        return pd.read_csv(url)

    def run(self):

        """
                        This run functions mainly created province-level cumulative confirmed&dead collection from
                        
                        province_confirmed_fetch and province_dead_fetch;
        
        """
        
        province_confirmed_data = self.province_confirmed_fetch()
        province_dead_data = self.province_dead_fetch()
        
        ### Get province names list
        province_list = list(province_confirmed_data["Unnamed: 1"])[1:]

        ### Get dates list
        time_list = list(province_confirmed_data.columns)[2:]
        
        ### Get country_level confirmed and dead case
        
        country_confirmed_array=np.array(province_confirmed_data.iloc[0,2:])
        country_dead_array=np.array(province_dead_data.iloc[0,2:])
        
        for j in range(len(time_list)):
            
            ### Translating data format from DD/MM to YYYY-MM-DD
            date_ddmm = time_list[j]
            date = datetime.strptime(date_ddmm + "/2020", '%d/%m/%Y').strftime('%Y-%m-%d')
            
            confirmed=country_confirmed_array[j]
            dead=country_dead_array[j]
            
            upsert_obj = {
                    # source is mandatory and is a code that identifies the  source
                    'source': 'BRA_MSHM',
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': "Brazil",
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'BRA',
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    # 'gid'=['BRA'],
                    'adm_area_1': None,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'confirmed': int(confirmed),
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': int(dead)

                }

            self.db.upsert_epidemiology_data(**upsert_obj)


            
            
        
        ### For province-level
        for k in range(len(time_list)):

            ### Translating data format from DD/MM to YYYY-MM-DD
            date_ddmm = time_list[k]
            date = datetime.strptime(date_ddmm + "/2020", '%d/%m/%Y').strftime('%Y-%m-%d')

            ### Get confirmed and dead list for current date
            current_confirm_list = np.array(province_confirmed_data[date_ddmm])
            current_dead_list = np.array(province_dead_data[date_ddmm])

            ### Fetch confirmed number and dead number for each province one by one
            for i in range(len(province_list)):
                province = province_list[i]
                confirmed = current_confirm_list[1 + i]
                dead = current_dead_list[1 + i]

                upsert_obj = {
                    # source is mandatory and is a code that identifies the  source
                    'source': 'BRA_MSHM',
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': "Brazil",
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'BRA',
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    'adm_area_1': province,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'confirmed': int(confirmed),
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': int(dead)

                }

                self.db.upsert_epidemiology_data(**upsert_obj)

