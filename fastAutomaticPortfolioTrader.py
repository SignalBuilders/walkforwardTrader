
# coding: utf-8

# In[ ]:

import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import numpy as np
import portfolioGeneration
import portfolio
import dataAck
import warnings
import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")
import multiprocessing as mp 
import autoPortfolioTree
import curveTreeDB
import portfolio
import random
factorToTrade = "VTI"

# In[ ]:
print("STARTING OBJECT DOWNLOAD")


def getPortfolioInputData():
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(params.validModelsCache)
            print("ATTEMPTING PULL", params.validModelsLookup)
            blob = storage.Blob(params.validModelsLookup, bucket)
            return pickle.loads(blob.download_as_string())
        except:
            return None
    pass

cleanedReturns, cleanedPredictions, hashToModel, joinedData = getPortfolioInputData()

def historicalWeightsToTickerAllocations(historicalWeights, algorithmPredictions, modelsInPortfolio):
    aggregatePredictions = algorithmPredictions.dropna()
    allocationsToStore = []
    historicalAllocations = None
    scaledHistoricalAllocations = None
    ##ITERATE THROUGH DAYS TO CALCULATE NET POSITION
    for i in range(len(historicalWeights)):
        netPosition = {}
        weights = historicalWeights.iloc[i]
        for model in modelsInPortfolio:
            if model.targetTicker not in netPosition:
                netPosition[model.targetTicker] = 0.0
            try:
                aggregatePredictions.loc[historicalWeights.index[i]]
            except:
                continue
            
            netPosition[model.targetTicker] += weights[model.getHash()] * aggregatePredictions.loc[historicalWeights.index[i]][model.getHash()]
        thisDf = pd.DataFrame([netPosition], index=[historicalWeights.index[i]])
        if historicalAllocations is None:
            historicalAllocations = thisDf
        else:
            historicalAllocations = pd.concat([historicalAllocations, thisDf])
        
        totalCapitalUsed = sum([abs(netPosition[ticker]) for ticker in netPosition])
        scaledNetPosition = {}
        for ticker in netPosition:
            scaledNetPosition[ticker] = netPosition[ticker] * 1.0/totalCapitalUsed
        
        thisDf = pd.DataFrame([scaledNetPosition], index=[historicalWeights.index[i]])
        if scaledHistoricalAllocations is None:
            scaledHistoricalAllocations = thisDf
        else:
            scaledHistoricalAllocations = pd.concat([scaledHistoricalAllocations, thisDf])
    
    return historicalAllocations, scaledHistoricalAllocations
            
            


# In[ ]:

import empyrical
def getLimitedDataForPortfolio(historicalWeights, historicalPredictions, modelsUsed, factorToTrade, joinedData):
    
    normalTickerAllocationsTable, scaledTickerAllocationsTable = historicalWeightsToTickerAllocations(historicalWeights, historicalPredictions, modelsUsed)
    
    tickerAllocationsTable = scaledTickerAllocationsTable
    tickerAllocationsTable = tickerAllocationsTable.fillna(0)
    rawTickerPerformance = portfolioGeneration.calculatePerformanceForTable(tickerAllocationsTable, tickerAllocationsTable.columns, joinedData)
    
    rawTickerPerformance = rawTickerPerformance[~rawTickerPerformance.index.duplicated(keep='first')]
    

    rawAlgoPerformance = pd.DataFrame(rawTickerPerformance.apply(lambda x:sum(x), axis=1), columns=["Algo Return Without Commissions"])

    tickerPerformance, algoPerformance, algoTransactionCost =  portfolioGeneration.calculatePerformanceForAllocations(tickerAllocationsTable, joinedData)

    benchmark = factorToTrade
    factorReturn = dataAck.getDailyFactorReturn(benchmark, joinedData)
    factorReturn.columns = ["Factor Return (" + benchmark + ")"]
    algoPerformance.columns = ["Algo Return"]
    
    tickersUsed = []
    for mod in modelsUsed:
        tickersUsed.append(mod.targetTicker)
    
#     for ticker in tickersUsed:
#         thisFactorReturn = dataAck.getDailyFactorReturn(ticker, joinedData)
#         thisFactorReturn.columns = ["Factor Return (" + ticker + ")"]
#         alpha, beta = empyrical.alpha_beta(algoPerformance, thisFactorReturn)
#         print(ticker, beta)
    
    alpha, beta = empyrical.alpha_beta(algoPerformance, factorReturn)
    sharpe_difference = empyrical.sharpe_ratio(algoPerformance) - empyrical.sharpe_ratio(factorReturn)
    annualizedReturn = empyrical.annual_return(algoPerformance)[0]
    annualizedVolatility = empyrical.annual_volatility(algoPerformance)
    
    ##AUTOMATICALLY TAKES SLIPPAGE INTO ACCOUNT
    return {
        "benchmark":factorToTrade,
        "alpha":alpha,
        "beta":beta,
        "sharpe difference":sharpe_difference,
        "annualizedReturn":annualizedReturn,
        "annualizedVolatility":annualizedVolatility,
        "sharpe":empyrical.sharpe_ratio(algoPerformance),
        "free return":annualizedReturn - annualizedVolatility
    }
    


# In[ ]:

def returnSelectAlgos(algoColumns):
    return np.random.choice(algoColumns, size=random.randint(15, len(algoColumns)), replace= False)


# In[ ]:

import hrpPortfolioOpt as hrp
def produceHRPPredictions(aggregateReturns, windowSize, startIndex, maxWindowSize = False):
    hrpReturns = pd.DataFrame([])
    historicalWeights = pd.DataFrame([])
    i = windowSize
    if startIndex is not None:
        i = startIndex
    while i < len(aggregateReturns):
        corr = None
        cov = None
        if maxWindowSize == False:
            corr = (aggregateReturns[:i]).corr()
            cov = (aggregateReturns[:i]).cov()
        else:
            corr = (aggregateReturns[i-windowSize:i]).corr()
            
            cov = (aggregateReturns[i-windowSize:i]).cov()
        try:
            weights = hrp.getHRP(cov, corr)
        #     display(weights)
        #     display(aggregateReturns[i+windowSize:i+windowSize+1])
            todayReturn = aggregateReturns[i:i+1] * weights
        #     display(todayReturn)
            sumReturn = pd.DataFrame(todayReturn.apply(lambda x:sum(x), axis=1))
            hrpReturns = pd.concat([hrpReturns, sumReturn])
            thisWeights = pd.DataFrame([[weights[item] for item in weights.index]], index=sumReturn.index, columns=weights.index.tolist())
            historicalWeights = pd.concat([historicalWeights, thisWeights])
        except:
            # print("FAILED:",i)
            pass
        i += 1
    return hrpReturns, historicalWeights


def produceEWPredictions(aggregateReturns, startIndex):
    historicalWeights = pd.DataFrame([])
    i = 0
    if startIndex is not None:
        i = startIndex
    columns = aggregateReturns.columns
    while i < len(aggregateReturns):
        todayReturn = aggregateReturns[i:i+1]
        thisWeights = pd.DataFrame([[1.0/len(columns) for item in columns]], index=todayReturn.index, columns=columns.tolist())
        historicalWeights = pd.concat([historicalWeights, thisWeights])
        i += 1
    return historicalWeights

def produceEWByTickerPredictions(aggregateReturns, startIndex, weights):

    historicalWeights = pd.DataFrame([])
    i = 0
    if startIndex is not None:
        i = startIndex
    columns = aggregateReturns.columns
    weightsKeys = list(weights.keys())
    while i < len(aggregateReturns):
        todayReturn = aggregateReturns[i:i+1]
        thisWeights = pd.DataFrame([[weights[item] for item in weightsKeys]], index=todayReturn.index, columns=columns.tolist())
        historicalWeights = pd.concat([historicalWeights, thisWeights])
        i += 1
    return historicalWeights

# In[ ]:

def storeDiscoveredPortfolio(models, portfolioType, benchmark, IS_DATA, OOS_DATA):
    description = "AUTO GENERATED"
    seenTickers = []
    
    allHashes = []
    for model in models:
        allHashes.append(model.getHash())
        if model.targetTicker not in seenTickers:
            seenTickers.append(model.targetTicker)
        
    ##SORT SO ENSURE SAME PORTFOLIO NOT CREATED TWICE
    allHashes = sorted(allHashes)
    
    portfolioString = str(allHashes) + benchmark + description + portfolioType
    portfolioHash = hashlib.sha224(portfolioString.encode('utf-8')).hexdigest()
    print("PORTFOLIO HASH:", portfolioHash)
    for hashing in allHashes:
        print(hashing)
        
    
        ##UPLOAD ORGANISM OBJECT
        while True:
            try:
                toUpload = {
                    "portfolio":portfolioHash,
                    "model":hashing
                }
                datastoreClient = datastore.Client('money-maker-1236')
                #HASH DIGEST
                key = datastoreClient.key(params.discoveredPortfolioModels, hashlib.sha224(str(hashing + portfolioHash).encode('utf-8')).hexdigest()) #NEED TO HASH TO ENSURE UNDER COUNT
                organismToStore = datastore.Entity(key=key)
                organismToStore.update(toUpload)
                datastoreClient.put(organismToStore)
                break
            except:
                print("UPLOAD ERROR:", str(sys.exc_info()))
                time.sleep(10)
    
    ##STORE PORTFOLIO OBJECT
    while True:
        try:
            toUpload = {
                "description":description,
                "benchmark":benchmark,
                "portfolioType":portfolioType,
                "startedTrading":curveTreeDB.getToday()
            }
            
            for k in IS_DATA:
                toUpload["IS_"+ k] = IS_DATA[k]
            
            for k in OOS_DATA:
                toUpload["OOS_"+ k] = OOS_DATA[k]
                
            for ticker in seenTickers:
                toUpload[ticker] = True
            
            toUpload["TICKERS TRADED"] = len(seenTickers)
            
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            key = datastoreClient.key(params.discoveredPortfolios, portfolioHash) #NEED TO HASH TO ENSURE UNDER COUNT
            organismToStore = datastore.Entity(key=key)
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            return portfolioHash
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)


# In[ ]:

def getWeightingForAlgos(allModels, columns):
    countPerTicker = {}
    hashes = {}
    for mod in allModels:
        hashes[mod.getHash()] = mod.targetTicker
        if mod.targetTicker not in countPerTicker:
            countPerTicker[mod.targetTicker] = 0.0
        countPerTicker[mod.targetTicker] += 1.0
    weightsToSend = []
    for col in columns:
        weightsToSend.append(1.0/countPerTicker[hashes[col]])
    
    weightDF = {}
    for i in range(len(columns)):
        weightDF[columns[i]] = weightsToSend[i]/sum(weightsToSend)

    return weightDF


# In[ ]:

def binarizeReturns(returnArr):
    newArr = []
    for item in returnArr:
        if item > 0.0:
            newArr.append(1.0)
        elif item < 0.0:
            newArr.append(-1.0)
        else:
            newArr.append(0.0)
    return newArr


# In[ ]:

def performPortfolioPerformanceEstimation(historicalPredictions, historicalReturns, factorToTrade, portfolioType, hashToModel, joinedData):
    returnWindows = [(0, historicalReturns[:450]), (450, historicalReturns)]
    historicalWeights = None
    allModels = [hashToModel[item] for item in historicalPredictions.columns]
    for selectedReturns in returnWindows:
        startIndex = selectedReturns[0]
        returnWindow = selectedReturns[1]
        weightsSeen = None
        if portfolioType == "HRP FULL":
            hrpReturns, weightsSeen = produceHRPPredictions(returnWindow, \
                126, startIndex=max(startIndex, 126), maxWindowSize=False)
        elif portfolioType == "HRP BINARY":
            hrpReturns, weightsSeen = produceHRPPredictions(pd.DataFrame(returnWindow.apply(lambda x:binarizeReturns(x),\
             axis=1)),                    126, startIndex=max(startIndex, 126), maxWindowSize=False)
        elif portfolioType == "HRP WINDOW":
            hrpReturns, weightsSeen = produceHRPPredictions(returnWindow, \
                126, startIndex=max(startIndex, 126), maxWindowSize=True)
        elif portfolioType == "EW":
            weightsSeen = produceEWPredictions(returnWindow, startIndex=max(startIndex, 126))
        elif portfolioType == "EW By Ticker":
            weights = getWeightingForAlgos(allModels, returnWindow.columns)
            weightsSeen = produceEWByTickerPredictions(returnWindow, startIndex=max(startIndex, 126), weights=weights)
            
        
        if historicalWeights is None:
            historicalWeights = weightsSeen
        else:
            historicalWeights = pd.concat([historicalWeights, weightsSeen])
        
        modelsUsed = []

        tickersSeen = {}

        for modelHash in historicalPredictions.columns:
            thisModel = hashToModel[modelHash]
            modelsUsed.append(thisModel)
        if startIndex == 0:
            scaledStats = getLimitedDataForPortfolio(historicalWeights,\
                        historicalPredictions, modelsUsed, factorToTrade, joinedData)
            print(scaledStats)
            if scaledStats["sharpe difference"] < 0.0 or scaledStats["annualizedReturn"] < scaledStats["annualizedVolatility"]:
                return None, None
    
    trainStats = getLimitedDataForPortfolio(historicalWeights[:-252], \
        historicalPredictions, modelsUsed, factorToTrade, joinedData)
    testStats = getLimitedDataForPortfolio(historicalWeights[-252:], \
        historicalPredictions, modelsUsed, factorToTrade, joinedData)
    
    if trainStats["sharpe difference"] > 0.0 and trainStats["annualizedReturn"] > trainStats["annualizedVolatility"]:
        print("ACCEPTED", trainStats, testStats)
        storeDiscoveredPortfolio(modelsUsed, portfolioType, factorToTrade, trainStats, testStats)
    else:
        print("FAILED", trainStats)
    


# In[ ]:

types =  ["HRP BINARY", "EW", "HRP WINDOW", "HRP FULL", "EW By Ticker"]


# In[ ]:

## MP RUN  
import time

def createPossiblePortfoliosMP(cleanedPredictions, cleanedReturns, hashToModel, joinedData, threadsToUse):
    mpEngine = mp.get_context('fork')
        
    runningP = []
    while True:
        selectedAlgorithms = returnSelectAlgos(cleanedReturns.columns)
        factorToTrade = "VTI"#hashToModel[selectedAlgorithms[random.randint(0, len(selectedAlgorithms) - 1)]].targetTicker
        
        while len(runningP) > threadsToUse:
            runningP = dataAck.cycleP(runningP)
            time.sleep(3)
            
        portfolioType = types[random.randint(0, len(types) - 1)]
        print(factorToTrade, len(selectedAlgorithms), portfolioType)
        
        p = mpEngine.Process(target=performPortfolioPerformanceEstimation, args=(cleanedPredictions[selectedAlgorithms],                    cleanedReturns[selectedAlgorithms], factorToTrade, portfolioType, hashToModel, joinedData))
        p.start()
        runningP.append(p)

# In[ ]:


# In[ ]:

print("STARTING GENERATION")

##REMOVE BREAK TO DO FULL AUTO
createPossiblePortfoliosMP(cleanedPredictions, cleanedReturns, hashToModel, joinedData, threadsToUse=1)


# In[ ]:



