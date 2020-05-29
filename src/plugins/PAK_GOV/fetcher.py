import logging
import pandas as pd
from utils.fetcher_abstract import AbstractFetcher
from datetime import datetime
import os
import sys
from selenium import webdriver
import time
from bs4 import BeautifulSoup
from .utils import isData, parseChartData

__all__ = ('PAK_GOV_Fetcher',)

logger = logging.getLogger(__name__)

"""
    Fetching data from Government of Pakistan
    http://covid.gov.pk/
    Data hosted on Google DataStudio
"""


class PAK_GOV_Fetcher(AbstractFetcher):
    LOAD_PLUGIN = True
    wd = None

    def wd_config(self):
        # configue a webdriver for selenium
        # this should probably be set at AbstractFetcher level
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        self.wd = webdriver.Chrome(
            executable_path=r'C:/Users/johnm/anaconda3/chromedriver/chromedriver.exe',
            chrome_options=chrome_options)

    def fetch(self, url):
        self.wd.get(url)
        # the site loads slowly, so wait until all content is present
        time.sleep(8)

        # the data is contained in chart labels - find all charts and extract their labels
        soup = BeautifulSoup(self.wd.page_source, "lxml")
        charts = soup.findAll("gviz-combochart") + soup.findAll("gviz-linechart") + soup.findAll("gviz-barchart")
        textTags = [[tag.text for tag in chart.findAll("text")] for chart in charts]

        # if a chart has the expected formatting, parse it
        dfList = [parseChartData(textTagList) for textTagList in textTags if isData(textTagList)]

        # merge the three parsed charts
        if len(dfList) != 3:
            raise Exception(f'Chart data could not be parsed at {url}')
        else:
            result = dfList[0].merge(dfList[1], on='Date').merge(dfList[2], on='Date')
        return result

    def province_fetcher(self, province, url):

        logger.info("Processing number of cases in " + province)

        df = self.fetch(url)

        for index, record in df.iterrows():
            # date must be reformatted
            d = record['Date']
            date = d.strftime('%Y-%m-%d')

            success, adm_area_1, adm_area_2, adm_area_3, gid = self.adm_translator.tr(
                input_adm_area_1=province,
                input_adm_area_2=None,
                input_adm_area_3=None,
                return_original_if_failure=True,
                suppress_exception=True
            )

            confirmed = record['Total Cases']
            dead = record['Total Deaths']
            recovered = record['Total Recoveries']

            # we need to build an object containing the data we want to add or update
            upsert_obj = {
                'source': 'PAK_GOV',
                'date': date,
                'country': 'Pakistan',
                'countrycode': 'PAK',
                'adm_area_1': adm_area_1,
                'adm_area_2': None,
                'adm_area_3': None,
                'gid': gid,
                'confirmed': confirmed,
                'recovered': recovered,
                'dead': dead
            }

            self.db.upsert_epidemiology_data(**upsert_obj)

    # read the list of countries from a csv file in order to fetch each one
    def load_provinces_to_fetch(self):
        input_csv_fname = getattr(self.__class__, 'INPUT_CSV', "input.csv")
        path = os.path.dirname(sys.modules[self.__class__.__module__].__file__)
        csv_fname = os.path.join(path, input_csv_fname)
        if not os.path.exists(csv_fname):
            return None
        colnames = ['province', 'url']
        input_pd = pd.read_csv(csv_fname)
        input_pd.columns = colnames
        input_pd = input_pd.where((pd.notnull(input_pd)), None)
        return input_pd

    def run(self):
        self.wd_config()
        provinces = self.load_provinces_to_fetch()
        total_failure = True
        for index, record in provinces.iterrows():
            try:
                self.province_fetcher(record['province'], record['url'])
                total_failure = False
            except:
                logging.warning(f'Fetcher failed for {province}, may work next time', exc_info=True)
        self.wd.quit()
        if total_failure:
            raise Exception('No provinces could be fetched for Pakistan')
