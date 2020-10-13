import re
import pandas as pd
from pandas import DataFrame
from fuzzywuzzy import fuzz

powiat_mapping = [
    ('górowski', 'Góra', 'Dolnośląskie'),
    ('lwówecki', 'Lwówek Śląski', 'Dolnośląskie'),
    ('średzki', 'Środa Śląska', 'Dolnośląskie'),
    ('chełmiński', 'Chełmno', 'Kujawsko-Pomorskie'),
    ('nakielski', 'Nakło'),
    ('sępoleński', 'Sępólno'),
    ('bialski', 'Biała Podlaska'),
    ('janowski', 'Janów Lubelski'),
    ('łęczyński', 'Łęczna'),
    ('opolski', 'Opole Lubelskie', 'Lubelskie'),
    ('radzyński', 'Radzyń Podlaski'),
    ('rycki', 'Ryki'),
    ('świdnicki', 'Świdnik', 'Lubelskie'),
    ('tomaszowski', 'Tomaszów Lubelski', 'Lubelskie'),
    ('krośnieński', 'Krosno Odrzańskie', 'Lubuskie'),
    ('tomaszowski', 'Tomaszów Mazowiecki', 'Łódzkie'),
    ('suski', 'Sucha'),
    ('grodziski', 'Grodzisk Mazowiecki', 'Mazowieckie'),
    ('nowodworski', 'Nowy Dwór Mazowiecki', 'Mazowieckie'),
    ('ostrowski', 'Ostrów Mazowiecka', 'Mazowieckie'),
    ('brzeski', 'Brzeg', 'Opolskie'),
    ('jasielski', 'Jasło'),
    ('krośnieński', 'Krosno', 'Podkarpackie'),
    ('moniecki', 'Mońki'),
    ('sokólski', 'Sokółka'),
    ('wysokomazowiecki', 'Wysokie Mazowieckie'),
    ('nowodworski', 'Nowy Dwór Gdański', 'Pomorskie'),
    ('bielski', 'Bielsko', 'Śląskie'),
    ('tarnogórski', 'Tarnowskie Góry', 'Śląskie'),
    ('konecki', 'Końskie', 'Świętokrzyskie'),
    ('nowomiejski', 'Nowe Miasto', 'Warmińsko-Mazurskie'),
    ('szczycieński', 'Szczytno', 'Warmińsko-Mazurskie'),
    ('grodziski', 'Grodzisk Wielkopolski', 'Wielkopolskie'),
    ('kępiński', 'Kępno', 'Wielkopolskie'),
    ('kolski', 'Koło', 'Wielkopolskie'),
    ('leszczyński', 'Leszno'),
    ('ostrowski', 'Ostrów Wielkopolski', 'Wielkopolskie'),
    ('pilski', 'Piła'),
    ('średzki', 'Środa Wielkopolska', 'Wielkopolskie'),
    ('sławieński', 'Sławno', 'Zachodniopomorskie'),
    ('gryficki', 'Gryfice', 'Zachodniopomorskie'),
    ('gryfiński', 'Gryfino', 'Zachodniopomorskie'),
]

city_mapping = [
    ('Łódź', 'Łódź'),
    (' st. Warszawa', 'Warszawa'),
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
        if region_name == 'POLSKA':
            return None, None, None, ['POL']

        df = self.administrative_division.copy()

        column = 'adm_area_1'
        voivodeship = True
        fuzzy = True
        if 'Powiat' in region_name:
            column = 'adm_area_2'
            voivodeship = False

            region_name = region_name.replace('Powiat ', '')

            for t in powiat_mapping:
                if region_name == t[0] and ((len(t) > 2 and adm_area_1 == t[2]) or len(t) == 2):
                    region_name = t[1]
                    fuzzy = False
                    break
            else:
                region_name = re.sub('ki$', '', region_name)

        if 'm.' in region_name:
            region_name = region_name.replace('m.', '')
            for t in city_mapping:
                if region_name == t[0] and ((len(t) > 2 and adm_area_1 == t[2]) or len(t) == 2):
                    region_name = t[1]
                    fuzzy = False
                    break
            else:
                region_name = region_name + ' (City)'

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
                condition = condition & (df.adm_area_1 == adm_area_1)

        result = df[condition]

        if len(result.index) == 0:
            raise Exception(f'Error, {region_name}, region not found!')
        if len(result.index) > 1:
            print(result['adm_area_2'].tolist())
            raise Exception(f'Error, {region_name}, too many regions matching: {result.index}')

        return result['adm_area_1'].tolist()[0], result['adm_area_2'].tolist()[0], \
               result['adm_area_3'].tolist()[0], result['gid'].tolist()
