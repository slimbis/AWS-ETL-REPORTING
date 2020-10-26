import pandas as pd

def getIndexes(mark, value):
    listOfPos = list()
    result = mark.isin([value])
    seriesObj = result.any()
    columnNames = list(seriesObj[seriesObj == True].index)
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
        for row in rows:
            listOfPos.append(col) #    listOfPos.append((row, col))
    s = [str(i) for i in listOfPos]
    res = int("".join(s))
    return res

def transform(cases,deaths,recovered):
    #cases = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv", header=None)
    #deaths = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv", header=None)
    #recovered = pd.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv", header=None)
    cases = cases.drop([0,2,3], axis=1)
    cases = cases.replace({'Country/Region':''})
    deaths = deaths.drop([0,2,3], axis=1)
    deaths = deaths.replace({'Country/Region':''})
    recovered = recovered.drop([0,2,3], axis=1)
    recovered = recovered.replace({'Country/Region':''})
    cases = cases.T
    deaths = deaths.T
    recovered = recovered.T
    #select country 'US' for USA, 'Greece' for Greece or other country from file
    #find specific country column in dataframe
    country_pos_1 = getIndexes(cases, 'Greece')
    country_pos_2 = getIndexes(deaths, 'Greece')
    country_pos_3 = getIndexes(recovered, 'Greece')

    #keep only selected country data (column)
    cases = cases.filter([0,country_pos_1])
    deaths = deaths.filter([0,country_pos_2])
    recovered = recovered.filter([0,country_pos_3])

    cases = cases.drop([1])
    deaths = deaths.drop([1])
    recovered = recovered.drop([1])

    cases.columns = ["date","cases"]
    deaths.columns = ["date","deaths"]
    recovered.columns = ["date","recovered"]
    cases['date'] = pd.to_datetime(cases['date'])
    deaths['date'] = pd.to_datetime(deaths['date'])
    recovered['date'] = pd.to_datetime(recovered['date'])
    intermediateDF = pd.merge(left=cases, right=deaths, left_on='date', right_on='date')
    finalDF = pd.merge(left=intermediateDF, right=recovered, left_on='date', right_on='date')
    cols = ['cases', 'deaths', 'recovered']
    finalDF[cols] = finalDF[cols].apply(pd.to_numeric, errors='coerce', axis=1)
    return finalDF
