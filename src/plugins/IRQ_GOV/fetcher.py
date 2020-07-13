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
    SOURCE = 'IRQ_GOV'  # United Arab Emirates Government 
    
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
        
        df = pd.DataFrame()        
          
        element = WebDriverWait(self.wd, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "visual.visual-columnChart.allow-deferred-rendering")))
        time.sleep(10)
                
        menu_btn = self.wd.find_element_by_xpath(".//*[@id='pbiAppPlaceHolder']/ui-view/div/div[2]/logo-bar/div/div/div/logo-bar-navigation/span/a[2]")
        menu_btn.click() 
        time.sleep(1)
        # go to the soecified page
        page_btn = self.wd.find_element_by_xpath(".//*[@id='flyoutElement']/div[1]/div/div/ul/li[3]")
        page_btn.click()
        time.sleep(1)    
        
        lst = []
        lst1 = []
        # find all the str column values in the table
        city = self.wd.find_elements_by_xpath(".//*[@id='pvExplorationHost']/div/div/exploration/div/explore-canvas-modern/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container-modern[1]/transform/div/div[3]/div/visual-modern/div/div/div[2]/div[1]/div[3]")
    
        for elem in city:
            a = elem.text
        lst = a.splitlines()   
        length = len(lst)   
        
        # find all numeric values in the table
        val = self.wd.find_elements_by_xpath(".//*[@id='pvExplorationHost']/div/div/exploration/div/explore-canvas-modern/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container-modern[1]/transform/div/div[3]/div/visual-modern/div/div/div[2]/div[1]/div[4]")
        
        for k in val:
             m = k.text
        lst1 = m.split('\n')
               
        sub_lst = [lst1[x:x+len(lst)] for x in range(0, len(lst1), len(lst))] 
        
        df = df.append(lst, ignore_index=True) 
        
        for i in range(len(sub_lst)):
            df.insert(i+1,"" ,sub_lst[i], True)
        
        df.columns = ['city', 'confirmed','recovered', 'dead', 'active']
        
        return df
        
        
    def run(self):
        self.wd_config()  
        logger.info("Processing provice data for Iraq")
        url = 'https://app.powerbi.com/view?r=eyJrIjoiNjljMDhiYmItZTlhMS00MDlhLTg3MjItMDNmM2FhNzE5NmM4IiwidCI6ImY2MTBjMGI3LWJkMjQtNGIzOS04MTBiLTNkYzI4MGFmYjU5MCIsImMiOjh9'
        
        data = self.fetch_province(url) 
                
        date = datetime.today().strftime('%Y-%m-%d')
        
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
                    'date': date,
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


