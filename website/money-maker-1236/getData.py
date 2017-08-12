import quandl
import pandas as pd
def getTickerData(ticker):
    ##RETURNS A DATAFRAME WITH ONLY THE COLUMNS WE CARE ABOUT
    limitedDf = quandl.get(["EOD/" + ticker +".11"], authtoken="G3AvFe4ZUZoBEthhjmEY")
    limitedDf.columns = ["Adj_Close_" + ticker]
    return limitedDf

def getDailyFactorReturn(ticker, joinedData):
    dailyFactorReturn = joinedData[["Adj_Close_" + ticker]].pct_change(1).shift(-1).dropna()
    dailyFactorReturn.columns = ["Factor Return"]
    return dailyFactorReturn