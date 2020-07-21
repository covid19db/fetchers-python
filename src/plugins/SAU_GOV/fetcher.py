# Copyright (C) 2020 University of Oxford
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import requests
from datetime import date
import pandas as pd
import datetime

__all__ = ('SAGOVFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class SAGOVFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data for Saudi Arabia https://covid19.moh.gov.sa/'''
    LOAD_PLUGIN = True
    SOURCE = 'SAU_GOV'
    
    def process_data(self,data):
        #loop through nested dictionaies and lists 
        df = pd.DataFrame()
        lst = []
        
        feat = data['features']  
        
        for i in range(len(feat)):
            attrib = feat[i]['attributes']
            lst.append(attrib.values())
        df = df.append(lst, ignore_index=True) 
        df.columns = feat[1]['attributes'].keys()
        
        return df
    
    # convert esriFieldTypeDate to ('%Y-%m-%d') 
    def convert_date(self,df):
        
        for i in range(len(df)): 
            dt_int = df.loc[i, "Reportdt"] / 1000.0
            dt = datetime.datetime.fromtimestamp(dt_int).strftime('%Y-%m-%d')
            df.loc[i, "Reportdt"] = dt
            
        return df
    
        
    #run_cases_by_date provides cumulative national level data by date 
    def fetch_by_date(self):
        logger.info("Processing cases by date for Saudi Arabia")
        
        url = 'https://services6.arcgis.com/bKYAIlQgwHslVRaK/arcgis/rest/services/Cumulative_Date_Grouped_ViewLayer/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        data = requests.get(url).json()        
        data = self.process_data(data)            
        data = self.convert_date(data) 
        
        
        for index, record in data.iterrows():
                confirmed = int(record['Confirmed'])
                dead = int(record['Deaths'])
                recovered = int(record['Recovered'])
                tested = int(record['Tested'])
                date = record['Reportdt']  

                upsert_obj = {
                   
                    'source': self.SOURCE,
                    'date': date,
                    'country': 'Saudi Arabia',
                    'countrycode': 'SAU',
                    'adm_area_1': None,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'confirmed': confirmed,
                    'dead': dead,
                    'recovered': recovered,
                    'tested': tested,
                    'gid': ['SAU']
                }
        
                self.upsert_data(**upsert_obj)
                
    # run_cases_by_region() provides cumulative daily regional (level 1) data 
    def fetch_by_region(self):
        logger.info("Processing cases by region for Saudi Arabia")
        
        url = 'https://services6.arcgis.com/bKYAIlQgwHslVRaK/arcgis/rest/services/CasesByRegion_ViewLayer/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        data = requests.get(url).json()        
        data = self.process_data(data)
                    
             
        #Last updated date is not present in the source
        date_ = date.today().strftime('%Y-%m-%d')   
        
        for index, record in data.iterrows():
                confirmed = int(record['TotalConfirmed'])
                dead = int(record['TotalDeaths'])
                recovered = int(record['TotalRecovered'])
                province = record['Region_Name_AR'] 
                
                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                        input_adm_area_1=province,
                        input_adm_area_2=None,
                        input_adm_area_3=None,
                        return_original_if_failure=True
                    )
             
                upsert_obj = {
                   
                    'source': self.SOURCE,
                    'date': date_,
                    'country': 'Saudi Arabia',
                    'countrycode': 'SAU',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'confirmed': confirmed,
                    'dead': dead,
                    'recovered': recovered,
                    'gid': gid
                }
        
                self.upsert_data(**upsert_obj)    
    
    def run(self):  
        
          self.fetch_by_date()   
          self.fetch_by_region()        
              
