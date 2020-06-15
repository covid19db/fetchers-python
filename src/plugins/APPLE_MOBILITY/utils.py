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

import io
import requests
import pandas as pd
from pandas import DataFrame
from datetime import date, timedelta
from requests.exceptions import ConnectionError


def get_recent_apple_mobility_data() -> DataFrame:
    baseurl = 'https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/en-us/applemobilitytrends-{}.csv'

    for delta in range(14):
        dt = date.today() - timedelta(delta)
        url = baseurl.format(dt.strftime("%Y-%m-%d"))
        try:
            request = requests.get(url, timeout=30)
            if request.status_code != 200:
                continue
            request.encoding = 'utf-8'
            return pd.read_csv(io.StringIO(request.text))
        except ConnectionError:
            continue
    return None
