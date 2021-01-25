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
import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

__all__ = ('IraqFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class IraqFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data for Iraq'''
    LOAD_PLUGIN = True
    SOURCE = 'IRQ_GOV'   
    
    def wd_config(self):
        # configue a webdriver for selenium
        # this should probably be set at AbstractFetcher level
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        self.wd = webdriver.Chrome('chromedriver',chrome_options=chrome_options)
        
    
    def fetch_province(self,url):
        self.wd.get(url)
        
        #create an empty dataframe
        df = pd.DataFrame()        
          
        element = WebDriverWait(self.wd, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "visual.visual-columnChart.allow-deferred-rendering")))
        time.sleep(10)
        
        #Get last updated date
        last_updated = self.wd.find_element_by_xpath(".//span[contains(text(), 'Updated on')]").text
        last_updated = last_updated.split(',')
        last_updated = last_updated[0]
        last_updated = last_updated.strip()
        last_updated = last_updated.replace('Updated on ','')
        date = datetime.strptime(last_updated, '%d %b %Y').strftime('%Y-%m-%d')
        
        #get the pages menu        
        menu_btn = self.wd.find_element_by_xpath(".//*[@id='embedWrapperID']/div[2]/logo-bar/div/div/div/logo-bar-navigation/span/a[3]")
        menu_btn.click() 
        time.sleep(5)   
        
        # go to the third page
        page_btn = self.wd.find_element_by_xpath(".//*[@id='embedWrapperID']/div[2]/logo-bar/div/div/div/logo-bar-navigation/span/a[3]")
        page_btn.click()
        time.sleep(5)    
           
        # find all the str column values in the table
        city = self.wd.find_element_by_xpath(".//*[name()='div' and @aria-label='COVID-19 Cumulative Status Matrix']//*[name()='div' and @class='rowHeaders']")
        city = city.text.splitlines()
       
        confirmed = self.wd.find_element_by_xpath(".//*[name()='div' and @aria-label='COVID-19 Cumulative Status Matrix']//*[name()='div' and @class='bodyCells']/div/div/div[1]")
        confirmed = confirmed.text.splitlines()
        
        recovered = self.wd.find_element_by_xpath(".//*[name()='div' and @aria-label='COVID-19 Cumulative Status Matrix']//*[name()='div' and @class='bodyCells']/div/div/div[2]")
        recovered = recovered.text.splitlines()
        
        dead = self.wd.find_element_by_xpath(".//*[name()='div' and @aria-label='COVID-19 Cumulative Status Matrix']//*[name()='div' and @class='bodyCells']/div/div/div[3]")
        dead = dead.text.splitlines()
        
        lst = []
        lst = list(zip(city, confirmed, recovered, dead))
        
        df = pd.DataFrame(lst, columns=['city', 'confirmed','recovered', 'dead'])
        
        # Baghdad is reported two rows from the source. the code below adds up 
        # the values from the two rows and creates a new total row for Baghdad
        # set city column is the index
        df.set_index('city', inplace =True)
        # select the two rows for Baghdad
        baghdad = df.loc[['BAGHDAD-KARKH','BAGHDAD-RESAFA'],:]
        baghdad[['confirmed','recovered', 'dead']] = baghdad[['confirmed','recovered', 'dead']].apply(pd.to_numeric)
        
        #add the new cumulative Bagdad sum to the DF
        df = df.append(baghdad.sum().rename('BAGHDAD'))
        
        #remove the two Baghdad rows from the original dataframe
        df = df.drop(['BAGHDAD-KARKH', 'BAGHDAD-RESAFA'])
        
        # reove the city column as index
        df.reset_index(inplace=True)
        
        self.wd.quit()
        
        return df,date
        
    
        
        
    def run(self):
        self.wd_config()  
        logger.info("Processing provice data for Iraq")
        url = 'https://app.powerbi.com/view?r=eyJrIjoiNjljMDhiYmItZTlhMS00MDlhLTg3MjItMDNmM2FhNzE5NmM4IiwidCI6ImY2MTBjMGI3LWJkMjQtNGIzOS04MTBiLTNkYzI4MGFmYjU5MCIsImMiOjh9'
        
        data,last_update = self.fetch_province(url)
         
        for index, record in data.iterrows():
                confirmed = int(record['confirmed'])
                dead = int(record['dead'])
                recovered = int(record['recovered'])
                province = record['city']    
                  
                
                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                        input_adm_area_1=province,
                        input_adm_area_2=None,
                        input_adm_area_3=None,
                        return_original_if_failure=True
                    )
                
                upsert_obj = {
                    'source': self.SOURCE,
                    'date': last_update,
                    'country': 'Iraq',
                    'countrycode': 'IRQ',
                    'adm_area_1': adm_area_1,
                    'adm_area_2': None,
                    'adm_area_3': None,
                    'gid': gid,
                    'confirmed': confirmed,
                    'recovered': recovered,
                    'dead': dead,
                }
        
                self.upsert_data(**upsert_obj)


