
import pandas as pd



def parser(data):
    """
    DESCRIPTION:
    A function to parse all data downloaded from CSV into the DB format.
    :param data: [pandas DataFrame] non-parsed data.
    :return: [pandas DataFrame] parsed data.
    """

    data = data.rename(columns={"CCAA": "adm_area_1", "fecha": "date", "casos": "confirmed", "UCI": "hospitalised_icu",
                         "Hospitalizados": "hospitalised", "curados": "recovered", "muertes": "dead"})

    data = data[['date', 'adm_area_1', 'confirmed', 'recovered', 'dead', 'hospitalised', 'hospitalised_icu']]

    data = data.where(pd.notnull(data), '')

    return data