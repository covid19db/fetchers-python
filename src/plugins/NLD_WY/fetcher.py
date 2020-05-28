from utils.fetcher_abstract import AbstractFetcher
import logging
import pandas as pd
import numpy as np

__all__ = ('NLD_WYFetcher',)

""" 
    site-location: https://github.com/J535D165/CoronaWatchNL
    
    COVID19-Netherlands Data for Netherlands created, maintained and hosted by CoronaWatchNL.
    
    The data sources include: National Institute for Public Health and the Environment 
        
    
"""
logger = logging.getLogger(__name__)


class NLD_WYFetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    SOURCE = 'NLD_WY'

    def country_fetch(self):

        """
                        This url mainly provide cumulative confirmed&dead&hospitalised data on the country-level.
        """

        url = 'https://raw.githubusercontent.com/J535D165/CoronaWatchNL/master/data/rivm_NL_covid19_national.csv'

        logger.debug('Fetching Netherland country-level confirmed-dead-hospitalised data from NLD_WY')

        return pd.read_csv(url)

    def run(self):

        """
                        This run function mainly created country-level cumulative confirmed-dead-hospitalised
                        collection from country_fetch;
        
        """

        country_data = self.country_fetch()

        ### Get dates & cumulative recorded lists
        time_list = list(country_data.Datum)
        lists = np.asarray(country_data.Aantal, dtype='int')

        for k in range(len(time_list)):

            ### Translating data format from DD/MM/YYYY to YYYY-MM-DD
            if k % 3 == 0:
                confirmed = lists[k]
                if confirmed < 0:
                    confirmed = None
                else:
                    confirmed = int(confirmed)
            elif k % 3 == 1:
                HOSPITALISED = lists[k]
                if HOSPITALISED < 0:
                    HOSPITALISED = None
                else:
                    HOSPITALISED = int(HOSPITALISED)
            else:
                date = time_list[k]
                dead = lists[k]
                if dead < 0:
                    dead = None
                else:
                    dead = int(dead)

                upsert_obj = {
                    # source is mandatory and is a code that identifies the  source
                    'source': self.SOURCE,
                    # date is also mandatory, the format must be YYYY-MM-DD
                    'date': date,
                    # country is mandatory and should be in English
                    # the exception is "Ships"
                    'country': "Netherlands",
                    # countrycode is mandatory and it's the ISO Alpha-3 code of the country
                    # an exception is ships, which has "---" as country code
                    'countrycode': 'NLD',
                    'gid': ['NLD'],
                    # adm_area_1, when available, is a wide-area administrative region, like a
                    # Canadian province in this case. There are also subareas adm_area_2 and
                    # adm_area_3
                    'adm_area_1': None,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'confirmed': confirmed,
                    # dead is the number of people who have died because of covid19, this is cumulative
                    'dead': dead,
                    'hospitalised': HOSPITALISED
                }

                self.db.upsert_epidemiology_data(**upsert_obj)
