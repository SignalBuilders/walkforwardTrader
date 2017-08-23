
# coding: utf-8

# In[25]:

import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import numpy as np
import portfolio
import dataAck
import warnings
import curveTreeDB
warnings.filterwarnings("ignore")
import portfolioGeneration
# from IPython.display import display
import datetime 
import autoPortfolioTree
startTime = datetime.datetime.now()


# In[26]:

modelHashes = curveTreeDB.getAllPortfolioModels()


# In[27]:

allModels = autoPortfolioTree.getModelsByKey(modelHashes)
modelsSeen = len(allModels)


# In[28]:

joinedData = curveTreeDB.getPertinentDataForModels(allModels)


# In[29]:

##SHOULD BE NAN IN MANY SERIES...DON'T WANT TO UNNECESSARILY TRUNCATE
# display(joinedData)


# In[30]:

autoPortfolioTree.generateRawPredictionsMP(allModels, joinedData, threadsToUse=20)


# In[31]:

##UPLOAD MODEL PREDICTIONS


# In[32]:

##STORE TODAY AGGREGATE FOR ALL MODELS
def produceModelPredictions(allModels, joinedData):
    for model in allModels:
        todayAggregatePrediction = curveTreeDB.getAggregatePredictionForModelDaily(model, joinedData)
        print(model.describe(), todayAggregatePrediction)
        dayToUpload = curveTreeDB.getToday()
        if dayToUpload == joinedData.index[-1]:
            print("SKIPPING UPLOAD, MUST WAIT UNTIL TOMORROW")
            continue
        curveTreeDB.storeAggregateModelPrediction(model, todayAggregatePrediction, curveTreeDB.getToday(), shouldReturn = False)


# In[33]:

produceModelPredictions(allModels, joinedData)


# In[34]:

##UPLOAD PORTFOLIO PREDICTIONS


# In[35]:

allPortfolios = portfolio.getPortfolios()
portfoliosSeen = len(allPortfolios)


# In[36]:

modelsInPortfolio = {}
portfolioTypes = {}
for portfolioInfo in allPortfolios:
    print(portfolioInfo)
    models = autoPortfolioTree.getModelsByKey(portfolio.getPortfolioModels(portfolioInfo["key"]))
    modelsInPortfolio[portfolioInfo["key"]] = models
    portfolioTypes[portfolioInfo["key"]] = portfolioInfo["portfolioType"]
    
    for model in models:
        print(model.describe())


# In[37]:

print(allModels)


# In[38]:

aggregateReturns, aggregatePredictions = autoPortfolioTree.generateAggregateReturnsPredictions(allModels, joinedData)


# In[39]:

# display(aggregatePredictions)


# In[40]:

##GENERATE WEIGHTS FOR PORTFOLIO TODAY
import pandas as pd
allocationsToStore = []
for portfolioKey in modelsInPortfolio:
    historicalWeights = None
    if portfolioTypes[portfolioKey] == "HRP FULL":
        print("HRP FULL")
        hrpReturns, historicalWeights = portfolioGeneration.produceHRPPredictions(aggregateReturns[[model.getHash() for model in modelsInPortfolio[portfolioKey]]], 126, startIndex=3, maxWindowSize=False)
    elif portfolioTypes[portfolioKey] == "HRP WINDOW":
        print("HRP WINDOW")
        hrpReturns, historicalWeights = portfolioGeneration.produceHRPPredictions(aggregateReturns[[model.getHash() for model in modelsInPortfolio[portfolioKey]]], 126, startIndex=3, maxWindowSize=True)
    elif portfolioTypes[portfolioKey] == "EW":
        print("EW")
        thisReturns = aggregateReturns[[model.getHash() for model in modelsInPortfolio[portfolioKey]]]
        historicalWeights = pd.DataFrame(thisReturns.apply(lambda x: [1.0/len(x) for item in x], axis=1), columns=thisReturns.columns.values)
    elif portfolioTypes[portfolioKey] == "EW By Ticker":
        print("EW By Ticker")
        thisReturns = aggregateReturns[[model.getHash() for model in modelsInPortfolio[portfolioKey]]]
        keptModels = []
        for mod in allModels:
            if mod.getHash() in thisReturns.columns:
                keptModels.append(mod)
        weightArray = portfolioGeneration.getWeightingForAlgos(keptModels, thisReturns.columns)
        historicalWeights = pd.DataFrame(thisReturns.apply(lambda x: weightArray, axis=1), columns=thisReturns.columns.values)
    print(portfolioKey, historicalWeights.iloc[-1])
    todayWeight = historicalWeights.iloc[-1]
    netPosition = {}
    transformedAlgoPrediction = {}
    for model in modelsInPortfolio[portfolioKey]:
        if model.targetTicker not in netPosition:
            netPosition[model.targetTicker] = 0.0
        netPosition[model.targetTicker] += todayWeight[model.getHash()] * curveTreeDB.getAggregatePredictionForModelDaily(model, joinedData)
        transformedAlgoPrediction[model.getHash()] = todayWeight[model.getHash()] * curveTreeDB.getAggregatePredictionForModelDaily(model, joinedData)
    print(portfolioKey, netPosition)
    allocationsToStore.append(portfolioGeneration.storePortfolioAllocation(portfolioKey, curveTreeDB.getToday(), todayWeight.to_dict(), netPosition, transformedAlgoPrediction, shouldReturn=True))
curveTreeDB.storeManyItems(allocationsToStore)



# In[42]:

## UPDATE CACHE FOR TRADING PORTFOLIOS
print("****UPDATING ALL PORTFOLIO CACHES****")
for mode in [params.AVAILABLE_MODE, params.PAPER_TRADING_MODE, params.TRADING_MODE]:
    print("MODE", mode)
    portfolioInfos = []
    downloadedPortfolioInfo = portfolioGeneration.getTradingPortfolioHashes(mode, includeDates = True)
    for tradingPortfolio in downloadedPortfolioInfo:
        portfolioHash = tradingPortfolio
        portfolioInfo = portfolio.getPortfolioByKey(portfolioHash)
        portfolioInfo = {
            "key":portfolioInfo.key.name,
            "description":portfolioInfo["description"],
            "benchmark":portfolioInfo["benchmark"],
            "portfolioType":portfolioInfo["portfolioType"],
            "startedTrading":downloadedPortfolioInfo[portfolioHash]
        }
        print(portfolioInfo)
        portfolioInfos.append(portfolioInfo)

    ##GET ALL BENCHMARKS
    benchmarksNeeded = []
    print(joinedData.columns.values)
    for info in portfolioInfos:
        print(info["benchmark"])
        if info["benchmark"] not in benchmarksNeeded and ("Adj_Close_" + info["benchmark"] not in joinedData.columns.values):
            benchmarksNeeded.append(info["benchmark"])
    print("BENCHMARKS NEEDED", benchmarksNeeded)
    pulledData, unused_ = dataAck.downloadTickerData(benchmarksNeeded)
    thisJoinedData = joinedData
    for ticker in pulledData:
        thisJoinedData = joinedData.join(pulledData[ticker], how='outer')
    for info in portfolioInfos:
        portfolioHash = info["key"]
        print(portfolioHash)
        portfolioData = autoPortfolioTree.getDataForPortfolio(portfolioHash, portfolioInfo["benchmark"], thisJoinedData, portfolioInfo["startedTrading"])
        portfolioGeneration.cachePortfolio(info, portfolioData, mode)




# # CALCULATE FUND PERFORMANCE METRICS

# In[43]:

# def cleanupAllocations(allocations):
#     allocationsToReturn = {}
#     for tradeDay in allocations:
#         totalAllocation = sum([abs(allocations[tradeDay][ticker]) for ticker in allocations[tradeDay]])
        
#         allocationsToReturn[tradeDay] = {}
#         for ticker in allocations[tradeDay]:
#             allocationsToReturn[tradeDay][ticker] = allocations[tradeDay][ticker]/totalAllocation
    
#     return allocationsToReturn
        
    


# # In[44]:

# import pandas as pd
# def createAllocationsTable(scaledAllocations):
#     allocationsTable = pd.DataFrame([])
#     for allocationDay in scaledAllocations:
#         keysInRow = list(scaledAllocations[allocationDay].keys())
#         allocationsTable = pd.concat([allocationsTable, pd.DataFrame([[scaledAllocations[allocationDay][key] for key in keysInRow]], index = [allocationDay], columns=keysInRow).tz_localize(None)])
#     return allocationsTable.sort_index().fillna(0)


# # In[45]:

# from datetime import timedelta
# def getNetAllocationAcrossPortfolios():
#     portfolioInfos = []
#     downloadedPortfolioInfo = portfolioGeneration.getTradingPortfolioHashes(params.TRADING_MODE, includeDates = True)
#     for tradingPortfolio in downloadedPortfolioInfo:
#         portfolioHash = tradingPortfolio
#         portfolioInfo = portfolio.getPortfolioByKey(portfolioHash)
#         portfolioInfo = {
#             "key":portfolioInfo.key.name,
#             "description":portfolioInfo["description"],
#             "benchmark":portfolioInfo["benchmark"],
#             "portfolioType":portfolioInfo["portfolioType"],
#             "startedTrading":downloadedPortfolioInfo[portfolioHash]
#         }
#         print(portfolioInfo)
#         portfolioInfos.append(portfolioInfo)
        
#     if len(portfolioInfos) == 0:
#         return None, None

#     #DAILY ALLOCATIONS MUST ONLY GET ALLOCATIONS FOR PORTFOLIO AFTER START DATE FOR PORTFOLIO
    
#     totalDesired = {}
#     historicalAllocations = {}
#     realizedAllocations = {}
#     for tradingPortfolio in portfolioInfos:
#         portfolioHash = tradingPortfolio["key"]
#         allAllocations = portfolio.getPortfolioAllocations(portfolioHash)
        
        
#         for item in allAllocations:
#             netPosition = {}
#             for key in item:
#                 if key.startswith("ticker_"):
#                     netPosition[key[len("ticker_"):]] = item[key]
            
#             ##REALIZED ALLOCATIONS
#             if item["predictionDay"] >= tradingPortfolio["startedTrading"]:
#                 if item["predictionDay"] not in realizedAllocations:
#                     realizedAllocations[item["predictionDay"]] = {}
#                 for ticker in netPosition:
#                     if ticker not in realizedAllocations[item["predictionDay"]]:
#                         realizedAllocations[item["predictionDay"]][ticker] = 0.0
#                     realizedAllocations[item["predictionDay"]][ticker] += netPosition[ticker]
            
#             if item["predictionDay"] not in historicalAllocations:
#                 historicalAllocations[item["predictionDay"]] = {}
#             for ticker in netPosition:
#                 if ticker not in historicalAllocations[item["predictionDay"]]:
#                     historicalAllocations[item["predictionDay"]][ticker] = 0.0
#                 historicalAllocations[item["predictionDay"]][ticker] += netPosition[ticker]
    
    
#     ##CLEANUP ALLOCATIONS TO SCALE TO CAPITAL 1
#     realizedAllocations = createAllocationsTable(cleanupAllocations(realizedAllocations))
#     historicalAllocations = createAllocationsTable(cleanupAllocations(historicalAllocations))       
#     return historicalAllocations, realizedAllocations
    


# # In[46]:

# import empyrical
# import json
# import portfolioGeneration
# def getFundData():
#     historicalAllocations, realizedAllocations =  getNetAllocationAcrossPortfolios()
#     if historicalAllocations is None:
#         return None, None
#     pulledData, unused_ = dataAck.downloadTickerData(historicalAllocations.columns.values)
#     allocationJoinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])
#     dataToCache = []
#     for allocationForm in [historicalAllocations, realizedAllocations]:
#         performanceByTicker, fundPerformance, fundTransactionCost = portfolioGeneration.calculatePerformanceForAllocations(allocationForm, allocationJoinedData)
#         if len(fundPerformance) == 0:
#             dataToCache.append({})
#             continue
        
#         ##CALCULATE BETAS FOR ALL TICKERS TO FUND PERFORMANCE
#         tickerAlphaBetas = []
#         for ticker in allocationForm.columns.values:
#             factorReturn = dataAck.getDailyFactorReturn(ticker, allocationJoinedData)
#             alpha, beta = empyrical.alpha_beta(fundPerformance, factorReturn)
#             tickerAlphaBetas.append({"ticker":ticker, "alpha":alpha * 100, "beta":beta})
        
#         tickerCols, tickerRows = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(performanceByTicker))
#         tickerAllocationsCols, tickerAllocationsRows = portfolioGeneration.convertTableToJSON(allocationForm)
#         fundCols, fundRows = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(fundPerformance))
        
#         sharpe = empyrical.sharpe_ratio(fundPerformance)
#         annualReturn = empyrical.annual_return(fundPerformance)[0]
#         annualVol = empyrical.annual_volatility(fundPerformance)
        
#         commissionCols, commissionRows = portfolioGeneration.convertTableToJSON(fundTransactionCost)
        
        
#         dataToCache.append({
#             "tickerAlphaBetas":tickerAlphaBetas,
#             "tickerCols":json.dumps(tickerCols),
#             "tickerRows":json.dumps(tickerRows),
#             "tickerAllocationsCols":json.dumps(tickerAllocationsCols),
#             "tickerAllocationsRows":json.dumps(tickerAllocationsRows),
#             "fundCols":json.dumps(fundCols),
#             "fundRows":json.dumps(fundRows),
#             "sharpe":sharpe,
#             "annualReturn":annualReturn * 100,
#             "annualVol":annualVol * 100,
#             "commissionCols":json.dumps(commissionCols),
#             "commissionRows":json.dumps(commissionRows)  
            
#         })
    
#     historicalData = dataToCache[0]
#     realizedData = dataToCache[1]
#     ##GET TODAY ALLOCATION
#     if realizedData != {}:
#         newRows = []
#         tARows = json.loads(realizedData["tickerAllocationsRows"])
#         tACols = json.loads(realizedData["tickerAllocationsCols"])
#         print(tARows[-1])
#         for i in range(len(tACols)):
            
#             newRows.append([tACols[i], abs(tARows[-1][i + 1])]) ##i+1 because date
#         realizedData["todayAllocation"] = json.dumps(newRows)
#         print(realizedData["todayAllocation"])
    
#     return historicalData, realizedData


# # In[47]:

# ##UPLOAD SO EASY TO READ
# import pickle
# from google.cloud import datastore, storage, logging
# def cacheFundData(historicalData, realizedData):
#     storageClient = storage.Client('money-maker-1236')
#     while True:
#         try:
#             bucket = storageClient.get_bucket(params.portfolioDataTradingCache) ##JUST SAVE IN TRADING BUCKET
#             blob = storage.Blob("HISTORICALFUND", bucket)
#             blob.upload_from_string(pickle.dumps(historicalData))
#             break
#         except:
#             print("UPLOAD BLOB ERROR:", str(sys.exc_info()))
#             time.sleep(10)
#     while True:
#         try:
#             bucket = storageClient.get_bucket(params.portfolioDataTradingCache) ##JUST SAVE IN TRADING BUCKET
#             blob = storage.Blob("REALIZEDFUND", bucket)
#             blob.upload_from_string(pickle.dumps(realizedData))
#             break
#         except:
#             print("UPLOAD BLOB ERROR:", str(sys.exc_info()))
#             time.sleep(10)
    
    


# # In[48]:

# historicalData, realizedData = getFundData()
# if historicalData is not None:
#     cacheFundData(historicalData, realizedData)


# # LOG COMPLETED TIME

# In[49]:

portfolioGeneration.storeSystemStatus(curveTreeDB.getToday(), startTime, datetime.datetime.now(), str(datetime.datetime.now() - startTime), modelsSeen=modelsSeen, portfoliosSeen=portfoliosSeen)


# In[ ]:



