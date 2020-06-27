# Copyright (C) 2020 Victor Vicente Palacios
# Copyright (C) 2020 University of Oxford
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# This file is derived from:
# https://github.com/victorvicpal/COVID19_es/blob/master/src/pdf_to_csv.py
#

import re
import numpy as np

###############################################
#                                             #
#    From PDF to CSV -- Based on structured   #
#    reports from Actualizacion_116           #
#                                             #
###############################################


def get_ccaa_tables(string, keywords):
    tabs = []
    for kw in keywords:
        i1 = string.find(kw)
        i1 = i1 + string[i1:].find('Andalucía')
        i2 = i1 + string[i1:].find('ESPAÑA')
        tabs.append(cleanlst(parse_list(get_lines(new_ccaa(string[i1:i2])))))
    return tabs


def get_fecha(string):
    ind_ini = string.find('(COVID')
    ind_fin = string.find('.20')
    return string[ind_ini + 12:ind_fin + 5]


def new_ccaa(text):
    dic = {'Castilla La Mancha': 'CastillaLaMancha', '.': '', ',': '.',
           'Castilla y León': 'CastillayLeón', '\n': '',
           'C. Valenciana': 'CValenciana',
           'País Vasco': 'PaísVasco',
           'La Rioja': 'LaRioja'}
    regex = re.compile("(%s)" % "|".join(map(re.escape, dic.keys())))
    return regex.sub(lambda mo: dic[mo.string[mo.start():mo.end()]], text)


def get_lines(string):
    ccaa_lst = [
        'Andalucía',
        'Aragón',
        'Asturias',
        'Baleares',
        'Canarias',
        'Cantabria',
        'CastillaLaMancha',
        'CastillayLeón',
        'Cataluña',
        'Ceuta',
        'CValenciana',
        'Extremadura',
        'Galicia',
        'Madrid',
        'Melilla',
        'Murcia',
        'Navarra',
        'PaísVasco',
        'LaRioja']

    lst = [string[string.find(ccaa):string.find(ccaa_lst[i + 1])]
           for i, ccaa in enumerate(ccaa_lst[:-1])]
    fin = [string[string.find('LaRioja'):]]

    return lst + fin


def parse_list(lst):
    lst = [el.split(' ') for el in lst]
    return lst


def hasNumbers(inputString):
    return bool(re.search(r'\d', inputString))


def hasCharacters(inputString):
    return bool(re.search(r'[a-zA-Zñáéíóú]+', inputString))


def justNumbers(inputString):
    return re.search(r'[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?', inputString).group()


def justCharacter(inputString):
    return re.search("[a-zA-Zñáéíóú]+", inputString).group()


def ind_empty_spc(lst):
    indices = [i for i, x in enumerate(lst) if x == ""]
    if len(indices) > 1:
        vect = np.array(indices)[1:] - np.array(indices)[:-1]
        inddel = [indices[i] for i in np.where(vect == 1)[0]][::-1]
        if inddel:
            return inddel


def cleanlst(lista):
    for i, l in enumerate(lista):
        inddel = ind_empty_spc(l)
        if inddel:
            [l.pop(i) for i in inddel]
        for j, el in enumerate(l):
            if hasNumbers(el):
                lista[i][j] = justNumbers(el)
            elif hasCharacters(el):
                lista[i][j] = justCharacter(el)
            else:
                lista[i][j] = el
    return lista
