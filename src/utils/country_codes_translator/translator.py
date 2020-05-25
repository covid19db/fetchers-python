import os
import logging
import pandas as pd
from pandas import DataFrame
from typing import Tuple, List

logger = logging.getLogger(__name__)


class CountryCodesTranslator:
    def __init__(self):
        self.translation_pd = self.load_translation_csv()

    def load_translation_csv(self) -> DataFrame:

        """
        DESCRIPTION:
        This function returns ISO country codes

        :return: [pandas DataFrame] ISO country codes.
        """
        translation_csv_fname = 'wikipedia-iso-country-codes.csv'
        path = os.path.dirname(__file__)
        return pd.read_csv(os.path.join(path, translation_csv_fname))

    def get_country_info(self, country_a2_code: str = None, country_name: str = None) -> Tuple:
        try:
            if country_a2_code and pd.notna(country_a2_code):
                country_info = \
                    self.translation_pd[self.translation_pd['Alpha-2 code'] == country_a2_code].to_dict('records')[0]
            else:
                country_info = \
                    self.translation_pd[self.translation_pd['English short name lower case'] == country_name].to_dict(
                        'records')[0]

            countrycode = country_info["Alpha-3 code"]
            country = country_info["English short name lower case"]
        except Exception as ex:
            return None, None

        return country, countrycode
