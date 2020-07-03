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

from datetime import datetime, timedelta
import pandas as pd


def isDate(element):
    # the dates later can sometimes appear as 'Mar 18, 20..'
    # So we will just rely on what comes before the comma
    element = element.split(',')[0]

    # check if the result has the format 'Mar 18' and store answer in correctDate
    correctDate = None
    try:
        newDate = datetime.strptime(element, '%b %d')
        correctDate = True
    except ValueError:
        correctDate = False
    return correctDate

def date_parser(date):
    ''' for expanding abbreviated dates'''
    try:
        return datetime.strptime(date, '%b %d, %Y')
    except ValueError:
        date = date.split(',')[0]
        date = datetime.strptime(date, '%b %d')

        current_year = datetime.today().year
        current_month = datetime.today().month

        if date.month in [11, 12] and current_month in [1,2]:
          date = date.replace(year = current_year - 1)
        else:
          date = date.replace(year = current_year)

        return date

def getTitle(chart_element):
  gtag = chart_element.find("g")
  chart_title=gtag.text
  return chart_title

def isData(chart_element):
    # the text for charts of interest begins with a header and then moves straight into dates
    return getTitle(chart_element) in ['Total Cases', 'Total Deaths', 'Total Recoveries']

def parseChartData(chart_element):
    # The title
    gtag = chart_element.find("g")
    title=gtag.text

    # The body data
    gtag = gtag.nextSibling
    textTags = gtag.findAll("text")
    textTagList = [tag.text for tag in textTags]

    # Starts with x-axis labels. These are dates - we can isolate the final date
    # Then y-axis labels are numbers - Large numbers will end in K
    # Smaller numbers can be found by failure of monotonicity
    # Then come the actual values

    idx_list = []
    size = len(textTagList)
    legendEndFound = False
    for idx, val in enumerate(textTagList):
        if isDate(val) and not isDate(textTagList[idx + 1]):
            idx_list.append(idx + 1)
        if val[-1] == 'K' and textTagList[idx + 1][-1] != 'K':
            idx_list.append(idx + 1)
            legendEndFound = True
    if not legendEndFound:
        for idx, val in enumerate(textTagList):
            if idx >= idx_list[-1]:
                if float(textTagList[idx + 1]) < float(val):
                    idx_list.append(idx + 1)
                    break

    # Then split the list up at these header values
    res = [textTagList[i: j] for i, j in
           zip([0] + idx_list, idx_list +
               ([size] if idx_list[-1] != size else []))]
    dates = res[0]
    values = res[2]

    # Turn the values into numbers
    values = [int(value.replace(',', '')) for value in values]

    # Replace the x-axis labels with a list of dates corresponding to values

    # We can extract the first date and determine what the last date should be
    firstDate = datetime.strptime(dates[0], '%b %d, %Y')
    d = timedelta(days=len(values) - 1)
    lastDate = firstDate + d

    # check the last date in the x-axis is what it should be
    lastDateCheck = dates[-1].split(',')[0]
    if not lastDateCheck == datetime.strftime(lastDate, '%b %-d'):
        raise Exception('date range does not match length of value list')

    # now generate the list of dates
    current = firstDate
    fullDateList = [current]
    while current != lastDate:
        current += timedelta(days=1)
        fullDateList.append(current)

    # the chart title is not consistently styled, so take the first two words
    #splitted = title.split()
    #title = " ".join(splitted[0:2])

    # build a dataframe with the dates and values, using chart title as column name
    df = pd.DataFrame()
    df['Date'] = fullDateList
    df[title] = values

    return df
