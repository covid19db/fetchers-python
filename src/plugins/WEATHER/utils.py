import numpy as np
import pandas as pd
import netCDF4
import datetime
import psycopg2
from requests import get
import os



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

# dowload the weather data for a single variable for all days in daterange
# use the adm_2_to_grid to assign each point in the grid to the right GID
# returns a pandas dataframe
def create_aggr_df(indicator, daterange, variables, adm_2_to_grid):
    days = []
    country = []
    avg = []
    std = []
    region = []
    city = []

    print("downloading data for {} from {} to {}".format(indicator,
                                                     daterange[0],
                                                     daterange[-1]))

    for day in daterange:
        URL = "https://metdatasa.blob.core.windows.net/covid19-response/metoffice_global_daily/"
        download_MET_file(URL+"{}/{}{}.nc".format(variables[indicator]['folder'],
                                                variables[indicator]['file'],
                                                day.strftime('%Y%m%d')),
                                                "plugins/WEATHER/temp/netCDF4_file.nc")

        nc = netCDF4.Dataset("plugins/WEATHER/temp/netCDF4_file.nc")

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

    d = {'day': days, 'country': country, 'region': region, 'city': city,
         indicator+'_avg': avg,
         indicator+'_std': std }

    return pd.DataFrame(data=d)
