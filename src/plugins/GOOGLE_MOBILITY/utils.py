import pandas as pd
from pandas import DataFrame


def get_country_codes():
    """
    DESCRIPTION:
    This function returns ISO country codes

    :return: [pandas DataFrame] ISO country codes.
    """
    return pd.read_csv('http://geohack.net/gis/wikipedia-iso-country-codes.csv')


def get_country_info(country_codes: DataFrame, country_a2_code: str = None, country_name: str = None):
    try:
        if country_a2_code and pd.notna(country_a2_code):
            country_info = country_codes[country_codes['Alpha-2 code'] == country_a2_code].to_dict('records')[0]
        else:
            country_info = \
                country_codes[country_codes['English short name lower case'] == country_name].to_dict('records')[0]

        countrycode = country_info["Alpha-3 code"]
        country = country_info["English short name lower case"]
    except Exception as ex:
        return None, None

    return country, countrycode
