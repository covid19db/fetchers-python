import pandas as pd


def get_country_codes():
    """
    DESCRIPTION:
    This function returns ISO country codes

    :return: [pandas DataFrame] ISO country codes.
    """
    return pd.read_csv('http://geohack.net/gis/wikipedia-iso-country-codes.csv')


def parser(api_data):
    """
    DESCRIPTION:
    This function paste out daily updates of govtrack data from JSON into DataFrame format.
    :param api_data: [JSON] non-parsed data.
    :return: [pandas DataFrame] parsed data.
    """
    records = []
    for item in api_data['data'].values():
        for record in item.values():
            records.append(record)

    govtrack_data = pd.DataFrame(records)
    govtrack_data.fillna(0, inplace=True)

    # Adding country name based on country code
    country_codes_data = get_country_codes()
    return govtrack_data.merge(country_codes_data, right_on='Alpha-3 code', left_on='country_code', how='left')
