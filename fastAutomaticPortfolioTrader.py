
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
import datetime 
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

    tickerPerformance, algoPerformance, algoTransactionCost =  portfolioGeneration.calculatePerformanceForAllocations(tickerAllocationsTable, joinedData)

    benchmark = factorToTrade
    factorReturn = dataAck.getDailyFactorReturn(benchmark, joinedData)
    factorReturn.columns = ["Factor Return (" + benchmark + ")"]
    algoPerformance.columns = ["Algo Return"]

    algoPerformanceRollingWeekly = algoPerformance.rolling(5, min_periods=5).apply(lambda x:empyrical.cum_returns(x)[-1]).dropna()
    algoPerformanceRollingWeekly.columns = ["Weekly Rolling Performance"]
    
    algoPerformanceRollingMonthly = algoPerformance.rolling(22, min_periods=22).apply(lambda x:empyrical.cum_returns(x)[-1]).dropna()
    algoPerformanceRollingMonthly.columns = ["Monthly Rolling Performance"]
    
    algoPerformanceRollingYearly = algoPerformance.rolling(252, min_periods=252).apply(lambda x:empyrical.cum_returns(x)[-1]).dropna()
    algoPerformanceRollingYearly.columns = ["Yearly Rolling Performance"]
    
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
    stability = empyrical.stability_of_timeseries(algoPerformance)
    profitability = len((algoPerformance.values)[algoPerformance.values > 0])/len(algoPerformance.values)
    


    rollingSharpe = algoPerformance.rolling(252, min_periods=252).apply(lambda x:empyrical.sharpe_ratio(x)).dropna()
    rollingSharpe.columns = ["252 Day Rolling Sharpe"]

    rollingSharpeError = rollingSharpe["252 Day Rolling Sharpe"].std()
    rollingSharpeMinimum = np.percentile(rollingSharpe["252 Day Rolling Sharpe"].values, 1)

    ##AUTOMATICALLY TAKES SLIPPAGE INTO ACCOUNT
    return {
        "benchmark":factorToTrade,
        "alpha":alpha,
        "beta":abs(beta),
        "sharpe difference":sharpe_difference,
        "annualizedReturn":annualizedReturn,
        "annualizedVolatility":annualizedVolatility,
        "sharpe":empyrical.sharpe_ratio(algoPerformance),
        "free return":annualizedReturn - annualizedVolatility,
        "stability":stability,
        "profitability":profitability,
        "rollingSharpeError":rollingSharpeError,
        "rollingSharpeMinimum":rollingSharpeMinimum,
        "weeklyMinimum":algoPerformanceRollingWeekly.min().values[0],
        "monthlyMinimum":algoPerformanceRollingMonthly.min().values[0],
        "yearlyMinimum":algoPerformanceRollingYearly.min().values[0]
    }, tickerAllocationsTable
    


# In[ ]:

def returnSelectAlgos(algoColumns):
    return np.random.choice(algoColumns, size=random.randint(15, len(algoColumns)), replace= False)

def getAlgosForTicker(allModels):
    modsPerTicker = {}
    for mod in allModels:
        if mod.targetTicker not in modsPerTicker:
            modsPerTicker[mod.targetTicker] = []
        modsPerTicker[mod.targetTicker].append(mod.getHash())
    return modsPerTicker

def returnSelectTickers(modsPerTicker):
    ##RANDOMLY CHOOSE SOME SUBSET OF TICKERS TO TRADE...TRADE ALL ALGORITHMS DISCOVERED WITHIN TICKER
    tickersAvailable = list(modsPerTicker.keys())
    chosenTickers = np.random.choice(np.array(tickersAvailable), size=random.randint(3, len(tickersAvailable)), replace= False)
    print(chosenTickers)
    modelsToUse = []
    for ticker in chosenTickers:
        modelsToUse += modsPerTicker[ticker]
    return modelsToUse

# In[ ]:

import cvxopt as opt
import cvxopt.solvers as optsolvers

def min_var_portfolio(cov_mat, allow_short=False):
    """
    Computes the minimum variance portfolio.

    Note: As the variance is not invariant with respect
    to leverage, it is not possible to construct non-trivial
    market neutral minimum variance portfolios. This is because
    the variance approaches zero with decreasing leverage,
    i.e. the market neutral portfolio with minimum variance
    is not invested at all.
    
    Parameters
    ----------
    cov_mat: pandas.DataFrame
        Covariance matrix of asset returns.
    allow_short: bool, optional
        If 'False' construct a long-only portfolio.
        If 'True' allow shorting, i.e. negative weights.

    Returns
    -------
    weights: pandas.Series
        Optimal asset weights.
    """
    if not isinstance(cov_mat, pd.DataFrame):
        raise ValueError("Covariance matrix is not a DataFrame")

    n = len(cov_mat)

    P = opt.matrix(cov_mat.values)
    q = opt.matrix(0.0, (n, 1))

    # Constraints Gx <= h
    if not allow_short:
        # x >= 0
        G = opt.matrix(-np.identity(n))
        h = opt.matrix(0.0, (n, 1))
    else:
        G = None
        h = None

    # Constraints Ax = b
    # sum(x) = 1
    A = opt.matrix(1.0, (1, n))
    b = opt.matrix(1.0)

    # Solve
    optsolvers.options['show_progress'] = False
    sol = optsolvers.qp(P, q, G, h, A, b)

    if sol['status'] != 'optimal':
        warnings.warn("Convergence problem")

    # Put weights into a labeled series
    weights = pd.Series(sol['x'], index=cov_mat.index)
    return weights


def tangency_portfolio(cov_mat, exp_rets, allow_short=False):
    """
    Computes a tangency portfolio, i.e. a maximum Sharpe ratio portfolio.
    
    Note: As the Sharpe ratio is not invariant with respect
    to leverage, it is not possible to construct non-trivial
    market neutral tangency portfolios. This is because for
    a positive initial Sharpe ratio the sharpe grows unbound
    with increasing leverage.
    
    Parameters
    ----------
    cov_mat: pandas.DataFrame
        Covariance matrix of asset returns.
    exp_rets: pandas.Series
        Expected asset returns (often historical returns).
    allow_short: bool, optional
        If 'False' construct a long-only portfolio.
        If 'True' allow shorting, i.e. negative weights.

    Returns
    -------
    weights: pandas.Series
        Optimal asset weights.
    """
    if not isinstance(cov_mat, pd.DataFrame):
        raise ValueError("Covariance matrix is not a DataFrame")

    if not isinstance(exp_rets, pd.Series):
        raise ValueError("Expected returns is not a Series")

    if not cov_mat.index.equals(exp_rets.index):
        raise ValueError("Indices do not match")

    n = len(cov_mat)

    P = opt.matrix(cov_mat.values)
    q = opt.matrix(0.0, (n, 1))

    # Constraints Gx <= h
    if not allow_short:
        # exp_rets*x >= 1 and x >= 0
        G = opt.matrix(np.vstack((-exp_rets.values,
                                  -np.identity(n))))
        h = opt.matrix(np.vstack((-1.0,
                                  np.zeros((n, 1)))))
    else:
        # exp_rets*x >= 1
        G = opt.matrix(-exp_rets.values).T
        h = opt.matrix(-1.0)

    # Solve
    optsolvers.options['show_progress'] = False
    sol = optsolvers.qp(P, q, G, h)

    if sol['status'] != 'optimal':
        warnings.warn("Convergence problem")

    # Put weights into a labeled series
    weights = pd.Series(sol['x'], index=cov_mat.index)

    # Rescale weights, so that sum(weights) = 1
    weights /= weights.sum()
    return weights

import hrpPortfolioOpt as hrp
def produceHRPPredictions(aggregateReturns, windowSize, startIndex, maxWindowSize = False):
    historicalWeights = pd.DataFrame([])
    i = windowSize
    if startIndex is not None:
        i = startIndex
    while i < len(aggregateReturns):
        corr = None
        cov = None
        if maxWindowSize == False:
            # startTime = datetime.datetime.now()
            corr = (aggregateReturns[:i]).corr()
            cov = (aggregateReturns[:i]).cov()
            # print("COV CORR", str(datetime.datetime.now() - startTime))
        else:
            corr = (aggregateReturns[i-windowSize:i]).corr()
            cov = (aggregateReturns[i-windowSize:i]).cov()
        try:
            # startTime = datetime.datetime.now()
            weights = hrp.getHRP(cov, corr)
            # print("WEIGHTS", str(datetime.datetime.now() - startTime))
        #     display(weights)
        #     display(aggregateReturns[i+windowSize:i+windowSize+1])
            todayReturn = aggregateReturns[i:i+1] * weights
        #     display(todayReturn)
            sumReturn = pd.DataFrame(todayReturn.apply(lambda x:sum(x), axis=1))
            thisWeights = pd.DataFrame([[weights[item] for item in weights.index]], index=sumReturn.index, columns=weights.index.tolist())
            historicalWeights = pd.concat([historicalWeights, thisWeights])
        except:
            # print("FAILED:",i)
            pass
        i += 1
    return historicalWeights

def produceMinVarPredictions(aggregateReturns, windowSize, startIndex, maxWindowSize = False):
    
    historicalWeights = pd.DataFrame([])
    i = windowSize
    if startIndex is not None:
        i = startIndex
    while i < len(aggregateReturns):
        corr = None
        cov = None
        if maxWindowSize == False:
            cov = (aggregateReturns[:i]).cov()
        else:
            cov = (aggregateReturns[i-windowSize:i]).cov()
        try:
            weights = min_var_portfolio(cov)
        #     display(weights)
        #     display(aggregateReturns[i+windowSize:i+windowSize+1])
            todayReturn = aggregateReturns[i:i+1] * weights
        #     display(todayReturn)
            sumReturn = pd.DataFrame(todayReturn.apply(lambda x:sum(x), axis=1))
            thisWeights = pd.DataFrame([[weights[item] for item in weights.index]], index=sumReturn.index, columns=weights.index.tolist())
            historicalWeights = pd.concat([historicalWeights, thisWeights])
        except:
            # print("FAILED:",i)
            pass
        i += 1
    return historicalWeights



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
            toUpload["ALGORITHMS TRADED"] = len(allHashes)
            
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

from google.cloud import error_reporting


def performPortfolioPerformanceEstimation(historicalPredictions, historicalReturns, factorToTrade, portfolioType, hashToModel, joinedData):
    client = error_reporting.Client('money-maker-1236', service="Curve Search", version=params.curveAndTreeVersion)

    try:
        returnWindows = [(0, historicalReturns[:600]), (600, historicalReturns)]
        historicalWeights = None
        allModels = [hashToModel[item] for item in historicalPredictions.columns]
        for selectedReturns in returnWindows:
            startIndex = selectedReturns[0]
            returnWindow = selectedReturns[1]
            weightsSeen = None
            if portfolioType == "HRP FULL":
                weightsSeen = produceHRPPredictions(returnWindow, \
                    126, startIndex=max(startIndex, 126), maxWindowSize=False)
            elif portfolioType == "HRP BINARY":
                weightsSeen = produceHRPPredictions(pd.DataFrame(returnWindow.apply(lambda x:binarizeReturns(x),\
                 axis=1)),                    126, startIndex=max(startIndex, 126), maxWindowSize=False)
            elif portfolioType == "HRP WINDOW":
                weightsSeen = produceHRPPredictions(returnWindow, \
                    126, startIndex=max(startIndex, 126), maxWindowSize=True)
            elif portfolioType == "EW":
                weightsSeen = produceEWPredictions(returnWindow, startIndex=max(startIndex, 126))
            elif portfolioType == "EW By Ticker":
                weights = getWeightingForAlgos(allModels, returnWindow.columns)
                weightsSeen = produceEWByTickerPredictions(returnWindow, startIndex=max(startIndex, 126), weights=weights)
            elif portfolioType == "MIN VAR":
                weightsSeen = produceMinVarPredictions(returnWindow, \
                    126, startIndex=max(startIndex, 126), maxWindowSize=False)
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
                scaledStats, unused = getLimitedDataForPortfolio(historicalWeights,\
                            historicalPredictions, modelsUsed, factorToTrade, joinedData)
                print(scaledStats)
                if scaledStats["sharpe difference"] < 0.0 or scaledStats["annualizedReturn"] < scaledStats["annualizedVolatility"]:
                    return None, None
        
        trainStats, tickerAllocationsTableTrain = getLimitedDataForPortfolio(historicalWeights[:-252], \
            historicalPredictions, modelsUsed, factorToTrade, joinedData)
        testStats, tickerAllocationsTableTest = getLimitedDataForPortfolio(historicalWeights[-252:], \
            historicalPredictions, modelsUsed, factorToTrade, joinedData)

        tickerAllocationsTable = pd.concat([tickerAllocationsTableTrain, tickerAllocationsTableTest])
        
        
        if trainStats["sharpe difference"] > 0.0 and trainStats["annualizedReturn"] > trainStats["annualizedVolatility"]:
            print("ACCEPTED", trainStats, testStats)
            portfolioHash = storeDiscoveredPortfolio(modelsUsed, portfolioType, factorToTrade, trainStats, testStats)
            curveTreeDB.storeFastPortfolio(portfolioHash, tickerAllocationsTable, historicalWeights, historicalPredictions)
        else:
            print("FAILED", trainStats)
    except:
        client.report_exception()
    


# In[ ]:

types =  ["HRP FULL", "EW", "EW By Ticker"]#["MIN VAR", "HRP BINARY", "EW", "HRP WINDOW", "HRP FULL", "EW By Ticker"]


# In[ ]:

## MP RUN  
import time

def createPossiblePortfoliosMP(cleanedPredictions, cleanedReturns, hashToModel, joinedData, threadsToUse):
    mpEngine = mp.get_context('fork')
        
    runningP = []
    allModels = [hashToModel[item] for item in cleanedPredictions.columns]
    modsPerTicker = getAlgosForTicker(allModels)
    while True:
        selectedAlgorithms = returnSelectTickers(modsPerTicker)
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
createPossiblePortfoliosMP(cleanedPredictions, cleanedReturns, hashToModel, joinedData, threadsToUse=3)


# In[ ]:



