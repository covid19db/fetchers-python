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
import time

from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from .utils import AR_TO_EN


__all__ = ('LebanonGovFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)




class LebanonGovFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data from Lebanon Ministry of Health'''
    LOAD_PLUGIN = True
    SOURCE = 'LBN_GOV'  # Lebanon Ministry of Health
    wd = None
     
    def wd_config(self):
        # configue a webdriver for selenium
        # this should probably be set at AbstractFetcher level
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        self.wd = webdriver.Chrome('chromedriver',chrome_options=chrome_options)
    
    
        
    
    # function appends the year to the date. This fucntion does not work if the cagrt dates go back more than 11 months
    def get_date(self,dt):
        
        yr = datetime.now().year
        current_month  = datetime.now().month
        month_name = dt[-3:]
        month_number = datetime.strptime(month_name, '%b').month
        if month_number > current_month:
            yr = yr - 1
        dt = dt + ' ' + str(yr)
        date = datetime.strptime(dt, '%d %b %Y').strftime('%Y-%m-%d')
        
        return date
    
    def fetch_regional(self,url):
        self.wd_config() 
        self.wd.get(url)      
        
        
        # wait until charts visible
        element = WebDriverWait(self.wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "apexcharts-canvas")))
        time.sleep(2)
        
        # Get last updated date
        last_update = self.wd.find_element_by_xpath(".//*[name()='h4' and @class='last-update']/strong").text
        last_update = last_update.split(',')
        last_update = last_update[0]
        last_update = last_update + ' ' + str(datetime.now().year)
        last_update = datetime.strptime(last_update, '%b %d %Y').strftime('%Y-%m-%d')
        
        chart = self.wd.find_element_by_xpath(".//*[name()='div' and @id='casesbydistricts']")
                
        bar_labels = chart.find_element_by_css_selector("g.apexcharts-datalabels")
        labels=bar_labels.text
        lst1=labels.split()
        
        bar_labels1 = chart.find_element_by_css_selector("g.apexcharts-yaxis-texts-g.apexcharts-xaxis-inversed-texts-g")
        provinces=bar_labels1.text
        lst2=provinces.split('\n')
        
        data= list(zip(lst1,lst2))
        df = pd.DataFrame(data, columns =['cases', 'province']) 
        self.wd.quit()
        return df,last_update
        
        

    def fetch_national(self,url):
        website_content = requests.get(url)
        soup = BeautifulSoup(website_content.text, 'lxml')
        lst = []
        Dict = {} 
        divs = soup.find_all('div', class_='counter-content')
        for elem in divs:
            h = elem.find('h1').get_text().strip()
            p = elem.find('p').get_text().strip()
            #spaces and newline characters. split and join
            AR_str = " ".join(p.split())
            #map the Arabic to the English translation
            EN_str = AR_TO_EN[AR_str]
            # Add to Dictionary
            Dict.update( {EN_str : h} )
          
        # NOTE : hospitalised_icu = Number of cases currently in ICU   
        df = pd.DataFrame({'confirmed':[Dict['confirmed'],],
                           'dead':[Dict['dead'],],
                           'recovered':[Dict['recovered'],],
                           'hospitalised_icu':[Dict['hospitalised_icu'],],
                           'tested':[Dict['tested'],],
                           'quarantined':[Dict['quarantined'],]})
      
        return df
    
    def fetch_historical(self,url):
        self.wd_config() 
        self.wd.get(url)
        res = []        
        
         # wait until charts visible
        element = WebDriverWait(self.wd, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "apexcharts-canvas")))
        time.sleep(2)
             
        # make a list of all charts
        chart_list = self.wd.find_elements_by_css_selector("div.apexcharts-canvas")
        chart=chart_list[0]

        # only works if chart is in view - mouse needs to scroll over element
        # this took me ages to figure out, but the rest of the code got better while it was happening!!
        self.wd.execute_script("arguments[0].scrollIntoView();", chart)         
      
        # iterate through the bars on the chart to hover on
        bar_list = chart.find_elements_by_xpath(".//*[name()='path' and @id='apexcharts-bar-area-0']")
        for el in bar_list:
            info = []
            hover = ActionChains(self.wd).move_to_element(el)
            hover.perform()
        
            # at this point the apexcharts-tooltip light element gets populated
            label = chart.find_element_by_css_selector("div.apexcharts-tooltip.light")
            d = label.find_element_by_css_selector("div.apexcharts-tooltip-title")
            date = self.get_date(d.text)
            info.append(date)
            data = label.find_elements_by_css_selector("div.apexcharts-tooltip-y-group")
            for datum in data:
              label_and_value = datum.find_elements_by_css_selector("span")
              if label_and_value:
                info.append(label_and_value[0].text)
                info.append(label_and_value[1].text)
            res.append(info)
        df = pd.DataFrame(res, columns=['date', 'daily', 'dailynum', 'total', 'totalnum'])
        self.wd.quit()
        return df
        
        
        
    def run(self):        
        
        url = 'https://corona.ministryinfo.gov.lb/'
        
        #Must run fetch_regional first as it gets last updated date
        logger.debug('Fetching regional level information')
        rdata, last_update = self.fetch_regional(url)

        for index, record in rdata.iterrows():
                confirmed = record[0]  
                province = record[1] 
                
                success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                        input_adm_area_1=province,
                        input_adm_area_2=None,
                        input_adm_area_3=None,
                        return_original_if_failure=True
                    )
                if adm_area_1 !='' :    
                    upsert_obj = {
                        'source': self.SOURCE,
                        'date': last_update,
                        'country': 'Lebanon',
                        'countrycode': 'LBN',
                        'adm_area_1': adm_area_1,
                        'adm_area_2': adm_area_2,
                        'adm_area_3': None,
                        'confirmed': confirmed,
                        'gid': gid
                    }
                    
                    self.upsert_data(**upsert_obj)
                    
        
        logger.debug('Fetching historical level information')
        hdata = self.fetch_historical(url)
        for index, record in hdata.iterrows():
                date = record['date']
                confirmed = record['totalnum']
                
                upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'Lebanon',
                'countrycode': 'LBN',
                'adm_area_1': None,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': ['LBN'],
                'confirmed': confirmed
                }
                
                self.upsert_data(**upsert_obj)
   
        
        logger.info("feching country-level information")        
        ndata = self.fetch_national(url)
        
        # date = datetime.today().strftime('%Y-%m-%d')
        dead = int(ndata.loc[ : , 'dead' ]) 
        confirmed = int(ndata.loc[ : , 'confirmed' ]) 
        recovered = int(ndata.loc[ : , 'recovered' ]) 
        tested = int(ndata.loc[ : , 'tested' ]) 
        hospitalised_icu = int(ndata.loc[ : , 'hospitalised_icu' ]) 
        quarantined = int(ndata.loc[ : , 'quarantined' ]) 
                   
        upsert_obj = {
            'source': self.SOURCE,
            'date': last_update,
            'country': 'Lebanon',
            'countrycode': 'LBN',
            'adm_area_1': None,
            'adm_area_2': None,
            'adm_area_3': None,
            'gid': ['LBN'],
            'confirmed': confirmed,
            'recovered': recovered,
            'dead': dead,
            'tested' : tested,
            'hospitalised_icu' : hospitalised_icu,
            'quarantined' : quarantined,
        }

        self.upsert_data(**upsert_obj)
    
        
        
        
                