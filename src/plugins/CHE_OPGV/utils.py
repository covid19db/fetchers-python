import pandas as pd


def parser(data):
    """
    DESCRIPTION:
    A function to parse all data downloaded from CSV into the DB format.
    :param data: [pandas DataFrame] non-parsed data.
    :return: [pandas DataFrame] parsed data.
    """

    data = data.rename(columns={"abbreviation_canton_and_fl": "adm_area_1", "date": "date", "ncumul_tested" : "tested", "ncumul_conf": "confirmed", "ncumul_ICU": "hospitalised_icu",
                                "ncumul_hosp": "hospitalised", "ncumul_deceased": "dead"})

    data = data[['date', 'adm_area_1', 'tested', 'confirmed', 'dead', 'hospitalised', 'hospitalised_icu']]

    data = data.where(pd.notnull(data), '')

    return data
