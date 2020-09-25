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


def date_parser(date):
    ''' for expanding abbreviated dates'''

    try:
        if '.' in date:
            res = datetime.strptime(date, '%d.%m.%Y')
        elif ',' in date:
            res = datetime.strptime(date, '%b %d, %Y')
        else:
            res = datetime.strptime(date, '%d %b %Y')
        return res
    except ValueError:
        if ',' in date:
            date = date.split(',')[0]
            date = datetime.strptime(date, '%b %d')
        else:
            date = ' '.join(date.split(' ')[0:-1])
            date = datetime.strptime(date, '%d %b')

        current_year = datetime.today().year
        current_month = datetime.today().month

        if date.month in [11, 12] and current_month in [1, 2]:
            date = date.replace(year=current_year - 1)
        else:
            date = date.replace(year=current_year)
        return date

def isDate(element):
    try:
        date_parser(element)
        return True
    except:
        return False

def getTitle(chart_element):
    gtag = chart_element.find("g")
    chart_title = gtag.text
    return chart_title


def isData(chart_element):
    # the text for charts of interest begins with a header and then moves straight into dates
    titleList = ['Total Cases', 'Total Deaths', 'Total Recoveries', 'Total Cases in Sindh', 'Total Deaths in Sindh', 'Total Recoveries in Sindh']
    return getTitle(chart_element) in titleList


def parseChartData(chart_element, debug_file):
    # The title
    gtag = chart_element.find("g")
    title = gtag.text

    # The body data
    body = gtag.nextSibling
    clipper = body.find("g")
    circles = clipper.nextSibling
    axisMarks = circles.nextSibling

    # Starts with x-axis labels. These are dates
    axisText = axisMarks.findAll("text")
    textTagList = [tag.text for tag in axisText]
    with open(debug_file, "a+") as outF:
        print(textTagList, file = outF)
    dates = [tag for tag in textTagList if isDate(tag)]

    # Next come values
    valuesTag = axisMarks.nextSibling
    valuesText = valuesTag.findAll("text")
    values = [tag.text for tag in valuesText]
    with open(debug_file, "a+") as outF:
        print(values, file = outF)

    # Turn the values into numbers
    values = [int(value.replace('.', '')) for value in values]

    # Replace the x-axis labels with a list of dates corresponding to values

    # We can extract the first date and determine what the last date should be
    firstDate = date_parser(dates[0])
    d = timedelta(days=len(values) - 1)
    lastDate = firstDate + d

    # check the last date in the x-axis is what it should be
    lastDateCheck = dates[-1].split(',')[0]
    if not date_parser(lastDateCheck) == lastDate:
        raise Exception('date range does not match length of value list')

    # now generate the list of dates
    current = firstDate
    fullDateList = [current]
    while current != lastDate:
        current += timedelta(days=1)
        fullDateList.append(current)

    # the chart title is not consistently styled, so take the first two words
    splitted = title.split()
    title = " ".join(splitted[0:2])

    # build a dataframe with the dates and values, using chart title as column name
    df = pd.DataFrame()
    df['Date'] = fullDateList
    df[title] = values

    return df
