import pandas as pd
from typing import Dict

from utils.country_codes_translator.translator import CountryCodesTranslator


def parser(api_data: Dict, country_codes_translator: CountryCodesTranslator):
    """
    DESCRIPTION:
    This function paste out daily updates of govtrack data from JSON into DataFrame format.
    :param api_data: [JSON] non-parsed data.
    :param country_codes_translator: [CountryCodesTranslator] country codes translator
    :return: [pandas DataFrame] parsed data.
    """
    records = []
    for item in api_data['data'].values():
        for record in item.values():
            records.append(record)

    govtrack_data = pd.DataFrame(records)
    govtrack_data.fillna(0, inplace=True)

    # Adding country name based on country code
    return govtrack_data.merge(country_codes_translator.translation_pd, right_on='Alpha-3 code', left_on='country_code',
                               how='left')


def to_int(data):
    return int(data) if pd.notna(data) else None
