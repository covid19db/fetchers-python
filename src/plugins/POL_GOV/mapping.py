import re
import pandas as pd
from pandas import DataFrame
from fuzzywuzzy import fuzz

powiat_mapping = [
    ('górowski', 'Góra', 'dolnośląskie'),
    ('lwówecki', 'Lwówek Śląski'),
    ('średzki', 'Środa Śląska', 'dolnośląskie'),
    ('chełmiński', 'Chełmno', 'kujawsko-pomorskie'),
    ('nakielski', 'Nakło'),
    ('sępoleński', 'Sępólno'),
    ('bialski', 'Biała Podlaska'),
    ('janowski', 'Janów Lubelski'),
    ('łęczyński', 'Łęczna'),
    ('opolski', 'Opole Lubelskie', 'lubelskie'),
    ('radzyński', 'Radzyń Podlaski'),
    ('rycki', 'Ryki'),
    ('świdnicki', 'Świdnik', 'lubelskie'),
    ('tomaszowski', 'Tomaszów Lubelski', 'lubelskie'),
    ('krośnieński', 'Krosno Odrzańskie', 'lubuskie'),
    ('tomaszowski', 'Tomaszów Mazowiecki', 'łódzkie'),
    ('suski', 'Sucha'),
    ('grodziski', 'Grodzisk Mazowiecki', 'mazowieckie'),
    ('nowodworski', 'Nowy Dwór Mazowiecki', 'mazowieckie'),
    ('ostrowski', 'Ostrów Mazowiecka', 'mazowieckie'),
    ('brzeski', 'Brzeg', 'opolskie'),
    ('jasielski', 'Jasło'),
    ('krośnieński', 'Krosno', 'podkarpackie'),
    ('moniecki', 'Mońki'),
    ('sokólski', 'Sokółka'),
    ('wysokomazowiecki', 'Wysokie Mazowieckie'),
    ('nowodworski', 'Nowy Dwór Gdański', 'pomorskie'),
    ('bielski', 'Bielsko', 'śląskie'),
    ('tarnogórski', 'Tarnowskie Góry', 'śląskie'),
    ('konecki', 'Końskie', 'świętokrzyskie'),
    ('nowomiejski', 'Nowe Miasto', 'warmińsko-mazurskie'),
    ('szczycieński', 'Szczytno', 'warmińsko-mazurskie'),
    ('grodziski', 'Grodzisk Wielkopolski', 'wielkopolskie'),
    ('kępiński', 'Kępno', 'wielkopolskie'),
    ('kolski', 'Koło', 'wielkopolskie'),
    ('leszczyński', 'Leszno'),
    ('ostrowski', 'Ostrów Wielkopolski', 'wielkopolskie'),
    ('pilski', 'Piła'),
    ('średzki', 'Środa Wielkopolska', 'wielkopolskie'),
    ('sławieński', 'Sławno', 'zachodniopomorskie'),
    ('gryficki', 'Gryfice', 'zachodniopomorskie'),
    ('gryfiński', 'Gryfino', 'zachodniopomorskie'),
]

city_mapping = [
    ('Łódź', 'Łódź'),
    # (' st. Warszawa', 'Warszawa'),
    ('Gdynia', 'Gdynia'),
    ('Sopot', 'Sopot'),
    ('Bytom', 'Bytom'),
    ('Zabrze', 'Zabrze'),
    ('Żory', 'Żory'),
    ('Inowrocław', 'Inowrocław'),
]


class RegionMapping:

    def __init__(self, conn):
        self.conn = conn
        self.administrative_division = self.get_administrative_division('POL')

    def get_administrative_division(self, countrycode: str = None) -> DataFrame:
        sql = f"SELECT * FROM administrative_division WHERE adm_level in (1,2) AND countrycode='{countrycode}'"
        return pd.read_sql_query(sql, self.conn)

    def find_nearest_translation(self, region_name: str, adm_area_1: str = None):
        if adm_area_1 == 'Cały kraj':
            return None, None, None, ['POL']

        df = self.administrative_division.copy()

        column = 'adm_area_1'
        voivodeship = True
        fuzzy = True
        if region_name[0].islower():
            column = 'adm_area_2'
            voivodeship = False

            for t in powiat_mapping:
                if region_name == t[0] and ((len(t) > 2 and adm_area_1.lower() == t[2].lower()) or len(t) == 2):
                    region_name = t[1]
                    fuzzy = False
                    break
            else:
                region_name = region_name.capitalize()
                region_name = re.sub('ki$', '', region_name)

        elif region_name[0].isupper():
            column = 'adm_area_2'
            voivodeship = False
            # City
            for t in city_mapping:
                if region_name == t[0] and ((len(t) > 2 and adm_area_1.lower() == t[2].lower()) or len(t) == 2):
                    region_name = t[1]
                    fuzzy = False
                    break
            else:
                region_name = region_name + ' (City)'
        else:
            raise Exception(f'Buuuu')


        def get_ratio(row):
            return fuzz.token_sort_ratio(row[column], region_name)

        if fuzzy:
            df['ratio'] = df.apply(get_ratio, axis=1)
            condition = (df.ratio == df.ratio.max())
        else:
            condition = df[column] == region_name

        if voivodeship:
            condition = condition & df.adm_area_2.isnull()
        else:
            if adm_area_1:
                condition = condition & (df.adm_area_1.str.lower() == adm_area_1.lower())

        result = df[condition]

        if len(result.index) == 0:
            raise Exception(f'Error, {region_name}, region not found!')
        if len(result.index) > 1:
            print(result['adm_area_2'].tolist())
            raise Exception(f'Error, {region_name}, too many regions matching: {result.index}')

        return result['adm_area_1'].tolist()[0], result['adm_area_2'].tolist()[0], \
               result['adm_area_3'].tolist()[0], result['gid'].tolist()
