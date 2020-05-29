from datetime import datetime, timedelta
import pandas as pd


def isHeader(element):
    # Tags beginning 'Total' are likely to indicate a new chart
    return element[:5] == 'Total'


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


def isData(textTagList):
    # the text for charts of interest begins with a header and then moves straight into dates
    return isHeader(textTagList[0]) and isDate(textTagList[1])


def parseChartData(textTagList):
    # We need to break up the chart data at header values

    # First find the header values and fill into idx_list
    # There is the title at the start
    # Then x-axis labels are dates - we can isolate the first date and final date
    # Then y-axis labels are numbers - Large numbers will end in K
    # Smaller numbers can be found by failure of monotonicity
    # At the end there appears to usually be a header value as well

    idx_list = []
    size = len(textTagList)
    legendEndFound = False
    for idx, val in enumerate(textTagList):
        if isDate(val) and not isDate(textTagList[idx - 1]):
            idx_list.append(idx)
        if isDate(val) and not isDate(textTagList[idx + 1]):
            idx_list.append(idx + 1)
        if val[-1] == 'K' and textTagList[idx + 1][-1] != 'K':
            idx_list.append(idx + 1)
            legendEndFound = True
    if not legendEndFound:
        for idx, val in enumerate(textTagList):
            if idx >= idx_list[-1]:
                if int(textTagList[idx + 1]) < int(val):
                    idx_list.append(idx + 1)
                    break
    if isHeader(textTagList[-1]):
        idx_list.append(size-1)

    # Then split the list up at these header values
    res = [textTagList[i: j] for i, j in
           zip([0] + idx_list, idx_list +
               ([size] if idx_list[-1] != size else []))]
    title = res[0][0]
    dates = res[1]
    values = res[3]

    # Turn the values into numbers
    values = [int(value.replace(',', '')) for value in values]

    # Replace the x-axis labels with a list of dates corresponding to values

    # We can extract the first date and determine what the last date should be
    firstDate = datetime.strptime(dates[0], '%b %d, %Y')
    d = timedelta(days=len(values) - 1)
    lastDate = firstDate + d

    # check the last date in the x-axis is what it should be
    lastDateCheck = dates[-1].split(',')[0]
    if not lastDateCheck == datetime.strftime(lastDate, '%b %#d'):
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
