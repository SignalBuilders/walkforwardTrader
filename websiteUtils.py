import portfolio
import dataAck
import pandas as pd
import empyrical

import json

def getPortfolios():
	return portfolio.getPortfolios()

def calculatePerformanceForTable(table, tickerOrder, joinedData):
    aggregatePerformance = None
    for i in range(len(tickerOrder)):
        dailyFactorReturn = dataAck.getDailyFactorReturn(tickerOrder[i], joinedData)
        thisPerformance = table[[table.columns[i]]].join(dailyFactorReturn).apply(lambda x:x[0] * x[1], axis=1)
        thisPerformance = pd.DataFrame(thisPerformance, columns=[table.columns[i]])
        if aggregatePerformance is None:
            aggregatePerformance = thisPerformance
        else:
            aggregatePerformance = aggregatePerformance.join(thisPerformance)
    return aggregatePerformance.dropna()

import time
def convertTableToJSON(table):
    allArrs = []
    for i in range(len(table.index)):
        thisArr = []
        timestamp = int(time.mktime(table.index[i].timetuple())) * 1000
        thisArr.append(timestamp)
        for j in range(len(table.columns)):
            thisArr.append(table.iloc[i][j])
        allArrs.append(thisArr)
    return table.columns.values.tolist(), allArrs

def getDataForPortfolio(portfolioKey):
    models = portfolio.getModelsByKey(portfolio.getPortfolioModels(portfolioKey))
    ##DOWNLOAD REQUIRED DATA FOR TARGET TICKERS
    tickersRequired = []
    for mod in models:
        print(mod.describe())
        if mod.inputSeries.targetTicker not in tickersRequired:
            tickersRequired.append(mod.inputSeries.targetTicker)

    pulledData, validTickers = dataAck.downloadTickerData(tickersRequired)

    joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])
    
    ##GENERATE RETURNS FOR PORTFOLIO
    portfolioAllocations = portfolio.getPortfolioAllocations(portfolioKey)
    
    predsTable = pd.DataFrame([])
    weightsTable = pd.DataFrame([])
    tickerAllocationsTable = pd.DataFrame([])
    scaledTickerAllocationsTable = pd.DataFrame([])
    for allocation in portfolioAllocations:
        colsAlgo = []
        valsAlgo = []
        colsAlgoWeight = []
        valsAlgoWeight = []
        colsTicker = []
        valsTicker = []
        colsTickerScaled = []
        valsTickerScaled = []

        for key in allocation:
            if key.startswith("ticker_"):
                colsTicker.append(key[len("ticker_"):])
                valsTicker.append(allocation[key])
            if key.startswith("scaled_ticker_"):
                colsTickerScaled.append(key[len("scaled_ticker_"):])
                valsTickerScaled.append(allocation[key])
            if key.startswith("algo_") and not key.startswith("algo_weight_"):
                colsAlgo.append(key[len("algo_"):])
                valsAlgo.append(allocation[key])
            if key.startswith("algo_weight_"):
                colsAlgoWeight.append(key[len("algo_weight_"):])
                valsAlgoWeight.append(allocation[key])

        predsTable = pd.concat([predsTable, pd.DataFrame([valsAlgo], index = [allocation["predictionDay"]], columns=colsAlgo).tz_localize(None)])
        weightsTable = pd.concat([weightsTable, pd.DataFrame([valsAlgoWeight], index = [allocation["predictionDay"]], columns=colsAlgoWeight).tz_localize(None)])
        tickerAllocationsTable = pd.concat([tickerAllocationsTable, pd.DataFrame([valsTicker], index = [allocation["predictionDay"]], columns=colsTicker).tz_localize(None)])
        scaledTickerAllocationsTable = pd.concat([scaledTickerAllocationsTable, pd.DataFrame([valsTickerScaled], index = [allocation["predictionDay"]], columns=colsTickerScaled).tz_localize(None)])
    
    predsTable = predsTable.sort_index()
    weightsTable = weightsTable.sort_index()
    tickerAllocationsTable = tickerAllocationsTable.sort_index()
    scaledTickerAllocationsTable = scaledTickerAllocationsTable.sort_index()
    
    tickerPerformance = calculatePerformanceForTable(tickerAllocationsTable, tickerAllocationsTable.columns, joinedData)
    
    algoPerformance = pd.DataFrame(tickerPerformance.apply(lambda x:sum(x), axis=1), columns=["Algo Return"])
    
    benchmark = portfolio.getPortfolioByKey(portfolioKey)["benchmark"]
    factorReturn = dataAck.getDailyFactorReturn(benchmark, joinedData)
    factorReturn.columns = ["Factor Return (" + benchmark + ")"]
    algoVsBenchmark = algoPerformance.join(factorReturn).dropna()
    
    ##FORM HASH TO TICKER
    hashToTicker = {}
    for model in models:
        hashToTicker[portfolio.getModelHash(model)] = model.inputSeries.targetTicker

    individualAlgoPerformance = calculatePerformanceForTable(predsTable,[hashToTicker[modelHash] for modelHash in predsTable.columns], joinedData)
    
    return json.dumps(convertTableToJSON(empyrical.cum_returns(tickerPerformance))),\
        json.dumps(convertTableToJSON(empyrical.cum_returns(algoPerformance))),\
        json.dumps(convertTableToJSON(empyrical.cum_returns(algoVsBenchmark))),\
        json.dumps(convertTableToJSON(empyrical.cum_returns(individualAlgoPerformance))),\
        