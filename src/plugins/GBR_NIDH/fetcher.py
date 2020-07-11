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
import time

from datetime import date, timedelta
from selenium import webdriver

__all__ = ('NorthernIrelandFetcher',)

from utils.fetcher.base_epidemiology import BaseEpidemiologyFetcher

logger = logging.getLogger(__name__)


class NorthernIrelandFetcher(BaseEpidemiologyFetcher):
    ''' a fetcher to collect data from Northern Ireland Department of Health'''
    LOAD_PLUGIN = True
    SOURCE = 'GBR_NIDH'  # Northern Ireland Department of Health
    wd = None
    yesterday = date.today() - timedelta(days=1)

    def parse_int(self, myString):
        if isinstance(myString, int):
            return myString
        else:
            return int(myString.replace(',', ''))

    def check_aria_label(self, item, phrase):
        label = item.get_attribute('aria-label')
        return label.startswith(phrase) if label else False

    def lau_label(self, label):
        lau = label.split(".")[0]
        prefix = 'Local Government District '
        return lau[len(prefix):]

    def lau_deaths(self, label):
        deaths = label.split()[-1]
        return self.parse_int(deaths[:-1])

    def wd_config(self):
        # configue a webdriver for selenium
        # this should probably be set at AbstractFetcher level
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        self.wd = webdriver.Chrome('chromedriver', chrome_options=chrome_options)
        self.wd.implicitly_wait(10)

    def fetch_national(self):

        # go to the national summary page
        contents_wrapper = self.wd.find_element_by_xpath(
            '//*[name()="div" and @class="visualContainerHost"]/visual-container-repeat')
        summary_page = contents_wrapper.find_element_by_xpath("./visual-container-modern[4]/transform")
        summary_page.click()

        # seems to need a fixed wait period before looking for the headline items
        time.sleep(5)
        items = self.wd.find_elements_by_xpath('//*[name()="visual-container-group"]//*[name()="svg"]')

        # pick out items by aria-label
        total_tested = [self.parse_int(item.text) for item in items if
                        self.check_aria_label(item, 'Individuals Tested')]
        confirmed = [self.parse_int(item.text) for item in items if
                     self.check_aria_label(item, 'Individuals with a Positive Test')]
        deaths = [self.parse_int(item.text) for item in items if self.check_aria_label(item, 'Num_Deaths')]

        upsert_obj = {
            'source': self.SOURCE,
            'date': self.yesterday.strftime('%Y-%m-%d'),
            'country': 'United Kingdom',
            'countrycode': 'GBR',
            'adm_area_1': 'Northern Ireland',
            'adm_area_2': None,
            'adm_area_3': None,
            'gid': ['GBR.2_1'],
            'confirmed': confirmed[0],
            'dead': deaths[0],
            'tested': total_tested[0]
        }

        self.upsert_data(**upsert_obj)

        # return home for next search
        home_button = self.wd.find_element_by_xpath('//visual-container-repeat/visual-container-modern[1]/transform')
        home_button.click()

    def fetch_regional(self):

        # go to the page giving tests by local government
        contents_wrapper = self.wd.find_element_by_xpath(
            '//*[name()="div" and @class="visualContainerHost"]/visual-container-repeat')
        local_govt_testing_page = contents_wrapper.find_element_by_xpath("./visual-container-modern[11]/transform")
        local_govt_testing_page.click()

        local_govt_table = self.wd.find_element_by_xpath(
            '//*[name()="div" and @aria-label="Breakdown of Individuals with a Laboratory Completed Test (Pillar 1 & '
            '2) by Local Government District Table"]//*[name()="div" and @class="bodyCells"]/div/div')
        columns = local_govt_table.find_elements_by_xpath('./div')

        local_testing_data = []
        for column in columns:
            column_data = []
            for element in column.find_elements_by_xpath('./div'):
                column_data.append(element.text)
            local_testing_data.append(column_data)

        data = list(zip(local_testing_data[0], map(self.parse_int, local_testing_data[1]),
                        map(self.parse_int, local_testing_data[2])))
        df_test = pd.DataFrame(data, columns=['lau', 'tests', 'positive'])

        home_button = self.wd.find_element_by_xpath('//visual-container-repeat/visual-container-modern[7]/transform')
        home_button.click()

        death_details_page = contents_wrapper.find_element_by_xpath("./visual-container-modern[21]/transform")
        death_details_page.click()
        time.sleep(5)
        chart_area = self.wd.find_element_by_xpath(
            '//div[@aria-label="COVID-19 Deaths by Local Government District Clustered column chart"]')
        rectangles = chart_area.find_elements_by_xpath(
            './/*[name()="svg"]/*[name()="svg"]//*[@class="series"]'
            '//*[name()="rect" and @class="column setFocusRing"]')
        labels = [rectangle.get_attribute('aria-label') for rectangle in rectangles]
        lau = [self.lau_label(label) for label in labels]
        lau_deaths = [self.lau_deaths(label) for label in labels]
        data = list(zip(lau, lau_deaths))
        df_deaths = pd.DataFrame(data, columns=['lau', 'deaths'])

        all_local_data = df_test.merge(df_deaths, on='lau')

        for index, record in all_local_data.iterrows():
            confirmed = record['positive']
            tested = record['tests']
            deaths = record['deaths']
            lgd = record['lau']
            date = self.yesterday

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1='Northern Ireland',
                input_adm_area_2=lgd,
                input_adm_area_3=None,
                return_original_if_failure=True,
                suppress_exception=True
            )

            upsert_obj = {
                'source': self.SOURCE,
                'date': date,
                'country': 'United Kingdom',
                'countrycode': 'GBR',
                'adm_area_1': adm_area_1,
                'adm_area_2': adm_area_2,
                'adm_area_3': None,
                'confirmed': confirmed,
                'dead': deaths,
                'tested': tested,
                'gid': gid
            }

            self.upsert_data(**upsert_obj)

            # do the upsert again for level 3

            if adm_area_2 != 'Unknown':
                upsert_obj['adm_area_3'] = adm_area_2
                upsert_obj['gid'] = [gid_element.split('_')[0] + '.1_1' for gid_element in gid]
                self.upsert_data(**upsert_obj)

    def run(self):

        # this website doesn't always load properly, so we'll take a few attempts
        url = 'https://app.powerbi.com/view?r=eyJrIjoiZGYxNjYzNmUtOTlmZS00ODAxLWE1YTEtMjA0NjZhMz'\
            'lmN2JmIiwidCI6IjljOWEzMGRlLWQ4ZDctNGFhNC05NjAwLTRiZTc2MjVmZjZjNSIsImMiOjh9'

        attempts = 1
        while attempts < 11:
            try:
                self.wd_config()
                logger.info("Fetching country-level information")
                self.wd.get(url)
                self.fetch_national()
                logger.debug('Fetching regional level information')
                self.fetch_regional()
                break
            except:
                logger.info(f'Failed on attempt {attempts}')
                attempts = attempts + 1
                if attempts == 10:
                    raise Exception('Could not fetch from Northern Ireland')

        self.wd.quit()
