import os
import logging
import pandas as pd
from pandas import DataFrame
from typing import Tuple, List

logger = logging.getLogger(__name__)


class AdmTranslator:
    def __init__(self, csv_fname: str):
        self.translation_pd = self.load_translation_csv(csv_fname)

    def load_translation_csv(self, csv_fname) -> DataFrame:
        # CSV without countrycode column
        colnames_1 = ['input_adm_area_1', 'input_adm_area_2', 'input_adm_area_3',
                      'adm_area_1', 'adm_area_2', 'adm_area_3', 'gid']
        # CSV with countrycode column
        colnames_2 = ['countrycode', 'input_adm_area_1', 'input_adm_area_2', 'input_adm_area_3',
                      'adm_area_1', 'adm_area_2', 'adm_area_3', 'gid']
        if not os.path.exists(csv_fname):
            return None
        translation_pd = pd.read_csv(csv_fname)
        translation_pd.columns = colnames_1 if len(translation_pd.columns) == len(colnames_1) else colnames_2
        translation_pd = translation_pd.where((pd.notnull(translation_pd)), None)
        return translation_pd

    def tr(self, country_code: str = None, input_adm_area_1: str = None, input_adm_area_2: str = None,
           input_adm_area_3: str = None, return_original_if_failure: bool = False,
           suppress_exception: bool = False) -> Tuple[bool, str, str, str, List]:

        for index, row in self.translation_pd.iterrows():
            if row.input_adm_area_1 == input_adm_area_1 \
                    and row.input_adm_area_2 == input_adm_area_2 \
                    and row.input_adm_area_3 == input_adm_area_3:

                if hasattr(row, 'countrycode') and country_code and row.countrycode != country_code:
                    continue

                if row.gid is None:
                    message = f'Unable to get GID for: {row.adm_area_1} {row.adm_area_2} {row.adm_area_3}'
                    raise Exception(message) if not suppress_exception else logging.warning(message)

                return True, row.adm_area_1, row.adm_area_2, row.adm_area_3, row.gid.split(':')
        if return_original_if_failure:
            return False, input_adm_area_1, input_adm_area_2, input_adm_area_3, None
        else:
            return False, None, None, None, None
