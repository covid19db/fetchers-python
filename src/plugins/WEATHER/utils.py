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
    # open in binary mode
    with open(file_name, "wb") as file:
        # get request
        response = get(url)
        # write to file
        file.write(response.content)
        file.close()


def load_local_data():
    # load the variables dict
    with open("plugins/WEATHER/input/weather_indicators.json", "r") as read_file:
        weather_indicators = json.load(read_file)

    # load grid to GADM level 2 dict
    with open('plugins/WEATHER/input/adm_2_to_grid.pkl', 'rb') as handle:
        adm_2_to_grid = pickle.load(handle)

    return weather_indicators, adm_2_to_grid


# dowload the weather data for a single variable for all days in daterange
# use the adm_2_to_grid to assign each point in the grid to the right GID
# returns a pandas dataframe
def create_aggr_df(indicator, day, variables, adm_2_to_grid, logger):
    days = []
    country = []
    avg = []
    std = []
    region = []
    city = []

    logger.debug("downloading data for {} for {}".format(indicator, day.strftime('%Y-%m-%d')))
    URL = "https://metdatasa.blob.core.windows.net/covid19-response/metoffice_global_daily/"
    temp_file = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'netCDF4_file.nc')
    download_MET_file("{}{}/{}{}.nc".format(URL, variables[indicator]['folder'], variables[indicator]['file'],
                                            day.strftime('%Y%m%d')), file_name=temp_file)

    nc = netCDF4.Dataset(temp_file)
    data = nc.variables[variables[indicator]['variable']][:].data.reshape(-1)

    for area_0 in adm_2_to_grid:
        for area_1 in adm_2_to_grid[area_0]:
            for area_2 in adm_2_to_grid[area_0][area_1]:
                idx_list = [point[0] for point in adm_2_to_grid[area_0][area_1][area_2]]

                to_avg = [data[idx] for idx in idx_list]

                days.append(day.strftime('%Y-%m-%d'))
                country.append(area_0)
                region.append(area_1)
                city.append(area_2)
                avg.append(np.mean(to_avg))
                std.append(np.std(to_avg))

    try:
        os.remove(temp_file)
    except:
        pass

    d = {'day': days, 'country': country, 'region': region, 'city': city,
         indicator + '_avg': avg, indicator + '_std': std}

    return pd.DataFrame(data=d)
