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

import os
import json
import pickle
import netCDF4
import numpy as np
import pandas as pd
from requests import get


# opening netCDF4 files via url is not reliable
# (it requires the package to be built with OPenDAP support)
# we dowload and write to disk the file before opening it
def download_MET_file(url, file_name):
    try:
        os.remove(file_name)
    except:
        pass
    # dowload the file from url and save it on disk
    # get request
    response = get(url)
    if response.status_code != 200:
        return False
    # open in binary mode
    with open(file_name, "wb") as file:
        # write to file
        file.write(response.content)
        file.close()
    return True


def load_local_data():
    # load the variables dict
    with open("plugins/WEATHER/input/weather_indicators.json", "r") as read_file:
        weather_indicators = json.load(read_file)

    # load grid to GADM level 1 dict
    with open('plugins/WEATHER/input/adm_1_info.pkl', 'rb') as handle:
        adm_1_info = pickle.load(handle)

    # load grid to GADM level 2 dict
    with open('plugins/WEATHER/input/adm_2_info.pkl', 'rb') as handle:
        adm_2_info = pickle.load(handle)

    return weather_indicators, adm_1_info, adm_2_info


# dowload the weather data for a single variable for all days in daterange
# use the adm_1_info and adm_2_info to assign each point in the grid to the right
# GID at level 1 or 2. the dicts also contains the GADM informations on each GID
# returns a pandas dataframe
def create_aggr_df(indicator, day, variables, adm_1_info, adm_2_info, logger):
    source = []
    date = []
    gid = []
    country = []
    countrycode = []
    adm_area_1 = []
    adm_area_2 = []
    adm_area_3 = []
    avg = []
    std = []
    samplesize = []
    valid_percentage = []

    logger.debug("downloading data for {} for {}".format(indicator, day.strftime('%Y-%m-%d')))
    URL = "https://metdatasa.blob.core.windows.net/covid19-response/metoffice_global_daily/"
    temp_file = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'netCDF4_file.nc')
    if not download_MET_file("{}{}/{}{}.nc".format(URL, variables[indicator]['folder'], variables[indicator]['file'],
                                                   day.strftime('%Y%m%d')), file_name=temp_file):
        return None

    nc = netCDF4.Dataset(temp_file)
    data = nc.variables[variables[indicator]['variable']][:].data.reshape(-1)

    if 'cloudaltitude' in indicator:
        # remove default values 9*10^36
        data[data > 10e20] = np.nan

    # Level 1 aggregation
    for area_0 in adm_1_info:
        for area_1 in adm_1_info[area_0]:
            idx_list = [point[0] for point in adm_1_info[area_0][area_1]["points"]]

            to_avg = [data[idx] for idx in idx_list]
            samplesize.append(len(to_avg))

            source.append("MET")
            date.append(day.strftime('%Y-%m-%d'))
            gid.append(adm_1_info[area_0][area_1]["gid"])
            country.append(adm_1_info[area_0][area_1]["country"])
            countrycode.append(adm_1_info[area_0][area_1]["countrycode"])
            adm_area_1.append(adm_1_info[area_0][area_1]["adm_area_1"])
            adm_area_2.append(adm_1_info[area_0][area_1]["adm_area_2"])
            adm_area_3.append(adm_1_info[area_0][area_1]["adm_area_3"])

            if 'cloudaltitude' in indicator:
                avg.append(np.nanmean(to_avg))
                std.append(np.nanstd(to_avg, ddof=1))
                valid_percentage.append(((~np.isnan(to_avg)).sum()) / (len(to_avg)))
            else:
                avg.append(np.mean(to_avg))
                std.append(np.std(to_avg, ddof=1))


    # Level 2 aggregation
    for area_0 in adm_2_info:
        for area_1 in adm_2_info[area_0]:
            for area_2 in adm_2_info[area_0][area_1]:
                idx_list = [point[0] for point in adm_2_info[area_0][area_1][area_2]["points"]]

                to_avg = [data[idx] for idx in idx_list]
                samplesize.append(len(to_avg))

                source.append("MET")
                date.append(day.strftime('%Y-%m-%d'))
                gid.append(adm_2_info[area_0][area_1][area_2]["gid"])
                country.append(adm_2_info[area_0][area_1][area_2]["country"])
                countrycode.append(adm_2_info[area_0][area_1][area_2]["countrycode"])
                adm_area_1.append(adm_2_info[area_0][area_1][area_2]["adm_area_1"])
                adm_area_2.append(adm_2_info[area_0][area_1][area_2]["adm_area_2"])
                adm_area_3.append(adm_2_info[area_0][area_1][area_2]["adm_area_3"])

                if 'cloudaltitude' in indicator:
                    avg.append(np.nanmean(to_avg))
                    std.append(np.nanstd(to_avg, ddof=1))
                    valid_percentage.append(((~np.isnan(to_avg)).sum()) / (len(to_avg)))
                else:
                    avg.append(np.mean(to_avg))
                    std.append(np.std(to_avg, ddof=1))


    if 'cloudaltitude' in indicator:
        d = {'source': source, 'date': date, 'gid': gid,
                 'country': country, 'countrycode': countrycode,
                 'adm_area_1': adm_area_1, 'adm_area_2': adm_area_2, 'adm_area_3': adm_area_3,
                 'samplesize': samplesize,
                 indicator+'_valid': valid_percentage,
                 indicator+'_avg': avg,
                 indicator+'_std': std,
                 }
    else:
        d = {'source': source, 'date': date, 'gid': gid,
             'country': country, 'countrycode': countrycode,
             'adm_area_1': adm_area_1, 'adm_area_2': adm_area_2, 'adm_area_3': adm_area_3,
             'samplesize': samplesize,
             indicator+'_avg': avg,
             indicator+'_std': std,
             }

    try:
        os.remove(temp_file)
    except:
        pass

    return pd.DataFrame(data=d)
