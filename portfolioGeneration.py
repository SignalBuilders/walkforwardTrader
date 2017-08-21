import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import dataAck
import portfolio

import numpy as np

def getUniqueModels(allModels):
    ##MUST PASS IN MODEL ENTITY
    ##GETS BEST MODEL FOR EACH INPUT SERIES
    sequences = {} 
    for modelEntity in allModels:
        model = modelEntity["model"]
        thisSeq = str(model.inputSeries.windowSize) + "," + \
                      str(model.inputSeries.series.describe())
        if thisSeq not in sequences:
            sequences[thisSeq] = []
        sequences[thisSeq].append({
            "model":model,
            "info":modelEntity
        })
    modelsToReturn = []
    modelEntities = []
    for seq in sequences:
        try:
            bestModel = None
            for modelInfo in sequences[seq]:
                if bestModel is None:
                    bestModel = modelInfo
                else:
                    if bestModel["info"]["RELATIVE SHARPE"] < modelInfo["info"]["RELATIVE SHARPE"]:
                        bestModel = modelInfo
            modelsToReturn.append(bestModel["model"])
            modelEntities.append(bestModel["info"])
        except:
            print(bestModel["model"].describe(), "MISSING")
    return modelsToReturn, modelEntities

def generateAllReturns(allModels, joinedData):
    aggregateReturns = None
    aggregatePredictions = None
    for mod in allModels:
        print(mod.describe())
        algoReturn, factorReturn, predictions =  mod.makePredictions(portfolio.prepareDataForModel(mod, joinedData))
        algoReturn.columns = [str(mod.describe())]
        predictions.columns = [str(mod.describe())]
        if aggregateReturns is None:
            aggregateReturns = algoReturn
            aggregatePredictions = predictions
        else:
            aggregateReturns = aggregateReturns.join(algoReturn)
            aggregatePredictions = aggregatePredictions.join(predictions)
    return aggregateReturns, aggregatePredictions

def visualizeModels(modelReturns):
    from string import ascii_letters
    import numpy as np
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt
    from IPython.display import display

    sns.set(style="white")


    # Compute the correlation matrix
    corr = modelReturns.corr()
    display(corr)

    # Generate a mask for the upper triangle
    mask = np.zeros_like(corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    from matplotlib.colors import ListedColormap

    # construct cmap
    flatui = ["#9b59b6", "#3498db", "#95a5a6", "#e74c3c", "#34495e", "#2ecc71"]
    cmap = ListedColormap(sns.color_palette(flatui).as_hex())
    
    # Draw the heatmap with the mask and correct aspect ratio
    sns.heatmap(corr, mask=mask, cmap=cmap, center=0,
                square=True, linewidths=.5, cbar_kws={"shrink": .5})
    plt.show()
    
    sns.set(style="white")
    
    # Compute the covariance matrix
    cov = modelReturns.cov()
    display(cov)
    # Generate a mask for the upper triangle
    mask = np.zeros_like(cov, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    flatui = ["#9b59b6", "#3498db", "#95a5a6", "#e74c3c", "#34495e", "#2ecc71"]
    cmap = ListedColormap(sns.color_palette(flatui).as_hex())

    # Draw the heatmap with the mask and correct aspect ratio
    sns.heatmap(cov, mask=mask, cmap=cmap, center=0,
                square=True, linewidths=.5, cbar_kws={"shrink": .5})
    plt.show()



def storePastPredictions(allModels, modelPredictions):
    ##THESE ARE SUMMED PREDICTIONS...DIFFERENT THAN PREDICTIONS MADE DAILY
    lastDayUsedPredictions = modelPredictions.dropna()
    allStoredModels = getAllPortfolioModels()
    print(allStoredModels)
    for i in range(len(lastDayUsedPredictions.columns)):
        ##CHECK IF ALREADY STORED
        
        thisModel = allModels[i]
        if portfolio.getModelHash(thisModel) in allStoredModels:
            print("SKIPPING", thisModel.describe())
            continue
        
        print(thisModel.describe())
        thisDF = lastDayUsedPredictions[[lastDayUsedPredictions.columns[i]]]
        predictionsToStore = []
        for j in range(len(thisDF.values)):
#             print(thisDF.index[i], thisDF.values[i][0])
            predictionsToStore.append(portfolio.storeAggregateModelPrediction(thisModel, thisDF.values[j][0], thisDF.index[j], shouldReturn=True))
        print("NEED TO STORE", len(predictionsToStore))
        portfolio.storeManyItems(predictionsToStore)
      

def getPortfolioHash(models, description, benchmark, portfolioType):
    allHashes = []
    for model in models:
        organismHash = hashlib.sha224(str(model.describe()).encode('utf-8')).hexdigest()
        allHashes.append(organismHash)
    
    allHashes.sort()
    portfolioString = str(allHashes) + description + benchmark + portfolioType
    return hashlib.sha224(portfolioString.encode('utf-8')).hexdigest()


def storePortfolio(models, description, benchmark, portfolioType):
    allHashes = []
    for model in models:
        organismHash = hashlib.sha224(str(model.describe()).encode('utf-8')).hexdigest()
        allHashes.append(organismHash)
    
    allHashes.sort()
    portfolioString = str(allHashes) + description + benchmark + portfolioType
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
                key = datastoreClient.key(params.portfolioDB, hashlib.sha224(str(hashing + portfolioHash).encode('utf-8')).hexdigest()) #NEED TO HASH TO ENSURE UNDER COUNT
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
                "startedTrading":portfolio.getToday()
            }
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            key = datastoreClient.key(params.portfolioLookup, portfolioHash) #NEED TO HASH TO ENSURE UNDER COUNT
            organismToStore = datastore.Entity(key=key)
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            return portfolioHash
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)

def getPortfolioModels(portfolioKey):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.portfolioDB)
            query.add_filter('portfolio', '=', portfolioKey)
            
            retrievedModels = [item["model"] for item in list(query.fetch())]

            return retrievedModels
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

##GET ALL MODELS PART OF PORTFOLIOS
def getAllPortfolioModels():
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.portfolioDB)
            
            retrievedModels = [item["model"] for item in list(query.fetch())]
            return list(set(retrievedModels))
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))


def getPertinentDataForModels(allModels):
    tickersRequired = []
    tickersTraded = []
    for mod in allModels:
        print(mod.describe())
        if mod.inputSeries.targetTicker not in tickersRequired:
            tickersRequired.append(mod.inputSeries.targetTicker)
        if mod.inputSeries.series.ticker not in tickersRequired:
            tickersRequired.append(mod.inputSeries.series.ticker)
        if mod.inputSeries.targetTicker not in tickersTraded:
            tickersTraded.append(mod.inputSeries.targetTicker)
        

    pulledData, validTickers = dataAck.downloadTickerData(tickersRequired)

    joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])
    return joinedData

def generateRawPredictions(allModels, joinedData, daysBack = False):
    for mod in allModels:
        pred = dataAck.computePosition([mod.makeTodayPrediction(portfolio.prepareDataForModel(mod, joinedData))])
        print(mod.describe(), pred, joinedData.index[-1])
        portfolio.storeModelPrediction(mod, pred, joinedData.index[-1])
        if daysBack == True:
            ##ENSURE POPULATED FOR CORRECT PREDICTION STYLE
            i = mod.inputSeries.predictionPeriod - 1
            while i > 0:
                pred = dataAck.computePosition([mod.makeTodayPrediction(joinedData[:-i])])
                print(mod.describe(), pred, joinedData[:-i].index[-1])
                portfolio.storeModelPrediction(mod, pred, joinedData[:-i].index[-1])
                i -= 1

import time
import multiprocessing as mp              
def generateRawPredictionsMP(allModels, joinedData, threadsToUse):
    mpEngine = mp.get_context('fork')
        
    def runMod(mod, joinedData):
        pred = dataAck.computePosition([mod.makeTodayPrediction(portfolio.prepareDataForModel(mod, joinedData))])
        print(mod.describe(), pred, joinedData.index[-1])
        portfolio.storeModelPrediction(mod, pred, joinedData.index[-1])
        ##ENSURE POPULATED FOR CORRECT PREDICTION STYLE
        i = mod.inputSeries.predictionPeriod - 1
        while i > 0:
            pred = dataAck.computePosition([mod.makeTodayPrediction(joinedData[:-i])])
            print(mod.describe(), pred, joinedData[:-i].index[-1])
            portfolio.storeModelPrediction(mod, pred, joinedData[:-i].index[-1])
            i -= 1
        
    runningP = []
    for mod in allModels:
        
        while len(runningP) > threadsToUse:
            runningP = dataAck.cycleP(runningP)
        
        p = mpEngine.Process(target=runMod, args=(mod, joinedData, ))
        p.start()
        runningP.append(p)


    while len(runningP) > 0:
            runningP = dataAck.cycleP(runningP)
    
    return True
        
                

from google.cloud import datastore, storage, logging
import time
import params
import hashlib
import pandas as pd

def downloadAggregatePredictions(model):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.aggregatePrediction)
            
            query.add_filter('modelHash', '=', hashlib.sha224((str(model.describe())).encode('utf-8')).hexdigest())
            retrievedPredictions = list(query.fetch())
            days = []
            predictions = []
            for pred in retrievedPredictions:
                days.append(pred["predictionDay"])
                predictions.append(pred["aggregatePrediction"])
            
            return pd.DataFrame(predictions, index=days, columns=[str(model.describe())]).sort_index()
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def generateAggregateReturnsPredictions(allModels, joinedData):
    aggregateReturns = None
    aggregatePredictions = None
    for model in allModels:
        preds = downloadAggregatePredictions(model).tz_localize(None)
        dailyFactorReturn = dataAck.getDailyFactorReturn(model.inputSeries.targetTicker, joinedData)
        transformedPreds = preds.join(dailyFactorReturn).dropna()
        returnStream = pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=[portfolio.getModelHash(model)])
        preds.columns = [portfolio.getModelHash(model)]
        if aggregateReturns is None:
            aggregateReturns = returnStream
            aggregatePredictions = preds
        else:
            aggregateReturns = aggregateReturns.join(returnStream)
            aggregatePredictions = aggregatePredictions.join(preds)
    return aggregateReturns, aggregatePredictions


def storePortfolioAllocation(portfolioKey, predictionDay, algorithmWeights, tickerAllocation, transformedAlgoPrediction, shouldReturn = False):
    toUpload = {}
    toUpload["portfolio"] = portfolioKey
    toUpload["predictionDay"] = predictionDay
    
    for item in algorithmWeights:
        toUpload["algo_weight_" + item] = algorithmWeights[item]
    
    for item in transformedAlgoPrediction:
        toUpload["algo_" + item] = transformedAlgoPrediction[item]
    
    for item in tickerAllocation:
        toUpload["ticker_" + item] = tickerAllocation[item]
        
    ##SCALE TICKER 
    totalAllocation = sum([abs(tickerAllocation[item]) for item in tickerAllocation])
    for item in tickerAllocation:
        toUpload["scaled_ticker_" + item] = tickerAllocation[item]/totalAllocation
    
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            predictionHash = hashlib.sha224((str(portfolioKey) + " " + str(toUpload["predictionDay"])).encode('utf-8')).hexdigest()
            key = datastoreClient.key(params.portfolioAllocation, predictionHash) #NEED TO HASH TO ENSURE NON-OVERLAPPING PREDICTIONS
            organismToStore = datastore.Entity(key=key)
            organismToStore.update(toUpload)
            if shouldReturn == False:
                datastoreClient.put(organismToStore)
            else:
                return organismToStore
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)

import hrpPortfolioOpt as hrp
def produceHRPPredictions(aggregateReturns, windowSize, startIndex, maxWindowSize = False):
    hrpReturns = pd.DataFrame([])
    historicalWeights = pd.DataFrame([])
    i = windowSize
    if startIndex is not None:
        i = len(aggregateReturns) - windowSize - startIndex
    while i < len(aggregateReturns):
        corr = None
        cov = None
        if maxWindowSize == False:
            corr = (aggregateReturns[:i]).corr()
            cov = (aggregateReturns[:i]).cov()
        else:
            corr = (aggregateReturns[i-windowSize:i]).corr()
            cov = (aggregateReturns[i-windowSize:i]).cov()
        weights = hrp.getHRP(cov, corr)
    #     display(weights)
    #     display(aggregateReturns[i+windowSize:i+windowSize+1])
        todayReturn = aggregateReturns[i:i+1] * weights
    #     display(todayReturn)
        sumReturn = pd.DataFrame(todayReturn.apply(lambda x:sum(x), axis=1))
        hrpReturns = pd.concat([hrpReturns, sumReturn])
        thisWeights = pd.DataFrame([[weights[item] for item in weights.index]], index=sumReturn.index, columns=weights.index.tolist())
        historicalWeights = pd.concat([historicalWeights, thisWeights])
        i += 1
    return hrpReturns, historicalWeights

def produceEWPredictions(aggregateReturns, startIndex):
    ewReturns = pd.DataFrame([])
    i = 0
    if startIndex is not None:
        i = len(aggregateReturns) - startIndex
    while i < len(aggregateReturns):
        todayReturn = aggregateReturns[i:i+1] * 1.0/len(aggregateReturns.columns.values)
        sumReturn = pd.DataFrame(todayReturn.apply(lambda x:sum(x), axis=1))
        ewReturns = pd.concat([ewReturns, sumReturn])
        i += 1
    return ewReturns

def getWeightingForAlgos(allModels, columns):
    countPerTicker = {}
    hashes = {}
    for mod in allModels:
        hashes[portfolio.getModelHash(mod)] = mod.inputSeries.targetTicker
        if mod.inputSeries.targetTicker not in countPerTicker:
            countPerTicker[mod.inputSeries.targetTicker] = 0.0
        countPerTicker[mod.inputSeries.targetTicker] += 1.0
    weightsToSend = []
    for col in columns:
        weightsToSend.append(1.0/countPerTicker[hashes[col]])
        
    return [item/sum(weightsToSend) for item in weightsToSend]

def storeHistoricalAllocations(portfolioKey, modelsInPortfolio, historicalWeights, aggregatePredictions):

    aggregatePredictions = aggregatePredictions.dropna()
    allocationsToStore = []
    ##ITERATE THROUGH DAYS TO CALCULATE NET POSITION
    for i in range(len(historicalWeights)):
        netPosition = {}
        weights = historicalWeights.iloc[i]
        transformedAlgoPrediction = {}
        for model in modelsInPortfolio:
            if model.inputSeries.targetTicker not in netPosition:
                netPosition[model.inputSeries.targetTicker] = 0.0
            try:
                aggregatePredictions.loc[historicalWeights.index[i]]
            except:
                continue
            
            netPosition[model.inputSeries.targetTicker] += weights[portfolio.getModelHash(model)] * aggregatePredictions.loc[historicalWeights.index[i]][portfolio.getModelHash(model)]
            transformedAlgoPrediction[portfolio.getModelHash(model)] = weights[portfolio.getModelHash(model)] * aggregatePredictions.loc[historicalWeights.index[i]][portfolio.getModelHash(model)]
        allocationsToStore.append(storePortfolioAllocation(portfolioKey, historicalWeights.index[i], weights.to_dict(), netPosition, transformedAlgoPrediction, shouldReturn=True))
    portfolio.storeManyItems(allocationsToStore)


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

import portfolio
import dataAck
import pandas as pd
import numpy as np
import json
import empyrical

def getDataForPortfolio(portfolioKey, factorToTrade, joinedData, availableStartDate):
    models = portfolio.getModelsByKey(portfolio.getPortfolioModels(portfolioKey))

    for model in models:
        print(model.describe())
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
                valsTickerScaled.append(abs(allocation[key]) if np.isnan(allocation[key]) == False else 0.0)
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
    weightsTable = weightsTable.sort_index().fillna(0)
    tickerAllocationsTable = tickerAllocationsTable.sort_index().fillna(0)
    scaledTickerAllocationsTable = scaledTickerAllocationsTable.sort_index().fillna(0)
    
    rawTickerPerformance = calculatePerformanceForTable(tickerAllocationsTable, tickerAllocationsTable.columns, joinedData)
    
    rawAlgoPerformance = pd.DataFrame(rawTickerPerformance.apply(lambda x:sum(x), axis=1), columns=["Algo Return Without Commissions"])
    
    tickerPerformance, algoPerformance, algoTransactionCost =  calculatePerformanceForAllocations(tickerAllocationsTable, joinedData)
    
    benchmark = portfolio.getPortfolioByKey(portfolioKey)["benchmark"]
    factorReturn = dataAck.getDailyFactorReturn(benchmark, joinedData)
    factorReturn.columns = ["Factor Return (" + benchmark + ")"]
    algoPerformance.columns = ["Algo Return"]
    algoVsBenchmark = algoPerformance.join(factorReturn).dropna()
    algoVsBenchmark = algoVsBenchmark.join(rawAlgoPerformance).dropna()

    tickerAlphaBetas = []
    for ticker in tickerAllocationsTable.columns.values:
        thisFactorReturn = dataAck.getDailyFactorReturn(ticker, joinedData)
        alpha, beta = empyrical.alpha_beta(algoPerformance, thisFactorReturn)
        tickerAlphaBetas.append({"ticker":ticker, "alpha":alpha * 100, "beta":beta})
        
        
    ##GET SCALED PERFORMANCE [FULL CAPITAL USED EACH DAY]
    rawTickerPerformanceScaled = calculatePerformanceForTable(scaledTickerAllocationsTable, scaledTickerAllocationsTable.columns, joinedData)
    
    rawAlgoPerformanceScaled = pd.DataFrame(rawTickerPerformanceScaled.apply(lambda x:sum(x), axis=1), columns=["Algo Return Without Commissions"])
    
    unused, algoPerformanceScaled, algoTransactionCostScaled =  calculatePerformanceForAllocations(scaledTickerAllocationsTable, joinedData)
    

    algoPerformanceScaled.columns = ["Algo Return"]
    algoVsBenchmarkScaled = algoPerformanceScaled.join(factorReturn).dropna()
    algoVsBenchmarkScaled = algoVsBenchmarkScaled.join(rawAlgoPerformanceScaled).dropna()
    
    
    
    ##FORM HASH TO TICKER
    hashToTicker = {}
    for model in models:
        hashToTicker[portfolio.getModelHash(model)] = model.inputSeries.targetTicker

    individualAlgoPerformance = calculatePerformanceForTable(predsTable,[hashToTicker[modelHash] for modelHash in predsTable.columns], joinedData)
    
    ##CONVERT TO USABLE OBJECTS
    tickerCols, tickerRows = convertTableToJSON(empyrical.cum_returns(tickerPerformance))
    tickerAllocationsCols, tickerAllocationsRows = convertTableToJSON(tickerAllocationsTable[-10:])
    algoCols, algoRows = convertTableToJSON(empyrical.cum_returns(algoPerformance))
    algoVsBenchmarkCols, algoVsBenchmarkRows = convertTableToJSON(empyrical.cum_returns(algoVsBenchmark))
    individualAlgoPerformanceCols, individualAlgoPerformanceRows = convertTableToJSON(empyrical.cum_returns(individualAlgoPerformance))
    scaledAllocationCols, scaledAllocationRows = convertTableToJSON(scaledTickerAllocationsTable)
    weightsCols, weightsRows = convertTableToJSON(weightsTable)
    alpha, beta = empyrical.alpha_beta(algoPerformance, factorReturn)
    recentAlpha, recentBeta = empyrical.alpha_beta(algoPerformance[-100:], factorReturn[-100:])
    recentSharpe = empyrical.sharpe_ratio(algoPerformance[-100:])
    recentReturn = empyrical.cum_returns(algoPerformance[-100:]).values[-1][0] * 100
    algoVsBenchmarkColsRecent, algoVsBenchmarkRowsRecent = convertTableToJSON(empyrical.cum_returns(algoVsBenchmark[-100:]))
    commissionCols, commissionRows = convertTableToJSON(algoTransactionCost)
    
    algoVsBenchmarkScaledCols, algoVsBenchmarkScaledRows = convertTableToJSON(empyrical.cum_returns(algoVsBenchmarkScaled))
    commissionScaledCols, commissionScaledRows = convertTableToJSON(algoTransactionCostScaled)
    scaledSharpe = empyrical.sharpe_ratio(algoPerformanceScaled)
    scaledReturn = empyrical.annual_return(algoPerformanceScaled)[0] * 100
    scaledVolatility = empyrical.annual_volatility(algoPerformanceScaled) * 100
    scaledAlpha, scaledBeta = empyrical.alpha_beta(algoPerformanceScaled, factorReturn)
    
    
    algoVsBenchmarkScaledColsRecent, algoVsBenchmarkScaledRowsRecent = convertTableToJSON(empyrical.cum_returns(algoVsBenchmarkScaled[-100:]))
    scaledSharpeRecent = empyrical.sharpe_ratio(algoPerformanceScaled[-100:])
    scaledReturnRecent = empyrical.annual_return(algoPerformanceScaled[-100:])[0] * 100
    scaledVolatilityRecent = empyrical.annual_volatility(algoPerformanceScaled[-100:]) * 100
    scaledAlphaRecent, scaledBetaRecent = empyrical.alpha_beta(algoPerformanceScaled[-100:], factorReturn[-100:])
    
    
    
    
    if len(algoPerformance[availableStartDate:]) > 0:
        ##NORMAL
        availableAlpha, availableBeta = empyrical.alpha_beta(algoPerformance[availableStartDate:], factorReturn[availableStartDate:])
        availableAlpha = availableAlpha * 100
        availableSharpe = empyrical.sharpe_ratio(algoPerformance[availableStartDate:])
        availableReturn = empyrical.cum_returns(algoPerformance[availableStartDate:]).values[-1][0] * 100
        algoVsBenchmarkColsAvailable, algoVsBenchmarkRowsAvailable = convertTableToJSON(empyrical.cum_returns(algoVsBenchmark[availableStartDate:]))
        
        ##SCALED
        availableAlphaScaled, availableBetaScaled = empyrical.alpha_beta(algoPerformanceScaled[availableStartDate:], factorReturn[availableStartDate:])
        availableAlphaScaled = availableAlphaScaled * 100
        availableSharpeScaled = empyrical.sharpe_ratio(algoPerformanceScaled[availableStartDate:])
        availableReturnScaled = empyrical.cum_returns(algoPerformanceScaled[availableStartDate:]).values[-1][0] * 100
        algoVsBenchmarkColsAvailableScaled, algoVsBenchmarkRowsAvailableScaled = convertTableToJSON(empyrical.cum_returns(algoVsBenchmarkScaled[availableStartDate:]))
    else:
        #NORMAL
        availableAlpha, availableBeta = ("NaN", "NaN")
        availableSharpe = "NaN"
        availableReturn = "NaN"
        algoVsBenchmarkColsAvailable, algoVsBenchmarkRowsAvailable = ([], [])
        
        #SCALED
        availableAlphaScaled, availableBetaScaled = ("NaN", "NaN")
        availableSharpeScaled = "NaN"
        availableReturnScaled = "NaN"
        algoVsBenchmarkColsAvailableScaled, algoVsBenchmarkRowsAvailableScaled = ([], [])

    return {
        "tickerCols":json.dumps(tickerCols),
        "tickerRows":json.dumps(tickerRows),
        "tickerAllocationsCols":json.dumps(tickerAllocationsCols),
        "tickerAllocationsRows":json.dumps(tickerAllocationsRows),
        "algoCols":json.dumps(algoCols),
        "algoRows":json.dumps(algoRows),
        "tickerCols":json.dumps(tickerCols),
        "tickerRows":json.dumps(tickerRows),
        "algoVsBenchmarkCols":json.dumps(algoVsBenchmarkCols),
        "algoVsBenchmarkRows":json.dumps(algoVsBenchmarkRows),
        "individualAlgoPerformanceCols":json.dumps(individualAlgoPerformanceCols),
        "individualAlgoPerformanceRows":json.dumps(individualAlgoPerformanceRows),
        "scaledAllocationCols":json.dumps(scaledAllocationCols),
        "scaledAllocationRows":json.dumps(scaledAllocationRows),
        "weightsCols":json.dumps(weightsCols),
        "weightsRows":json.dumps(weightsRows),
        "algoSharpe":empyrical.sharpe_ratio(algoPerformance),
        "alpha":alpha * 100,
        "beta":beta,
        "annualReturn":empyrical.annual_return(algoPerformance)[0] * 100,
        "annualVolatility":empyrical.annual_volatility(algoPerformance) * 100,
        "recentSharpe":recentSharpe,
        "recentReturn":recentReturn,
        "recentAlpha":recentAlpha * 100,
        "recentBeta":recentBeta,
        "algoVsBenchmarkColsRecent":json.dumps(algoVsBenchmarkColsRecent),
        "algoVsBenchmarkRowsRecent":json.dumps(algoVsBenchmarkRowsRecent),
        "commissionCols":json.dumps(commissionCols),
        "commissionRows":json.dumps(commissionRows),
        "tickerAlphaBetas":tickerAlphaBetas,
        "availableAlpha":availableAlpha,
        "availableBeta":availableBeta,
        "availableSharpe":availableSharpe,
        "availableReturn":availableReturn,
        "algoVsBenchmarkColsAvailable":json.dumps(algoVsBenchmarkColsAvailable),
        "algoVsBenchmarkRowsAvailable":json.dumps(algoVsBenchmarkRowsAvailable),
        "algoVsBenchmarkScaledCols":json.dumps(algoVsBenchmarkScaledCols), 
        "algoVsBenchmarkScaledRows":json.dumps(algoVsBenchmarkScaledRows),
        "commissionScaledCols":json.dumps(commissionScaledCols), 
        "commissionScaledRows":json.dumps(commissionScaledRows),
        "scaledReturn":scaledReturn,
        "scaledSharpe":scaledSharpe,
        "scaledVolatility":scaledVolatility,
        "scaledAlpha":scaledAlpha * 100,
        "scaledBeta":scaledBeta,
        "algoVsBenchmarkScaledColsRecent":json.dumps(algoVsBenchmarkScaledColsRecent),
        "algoVsBenchmarkScaledRowsRecent":json.dumps(algoVsBenchmarkScaledRowsRecent),
        "scaledReturnRecent":scaledReturnRecent,
        "scaledVolatilityRecent":scaledVolatilityRecent,
        "scaledAlphaRecent":scaledAlphaRecent * 100,
        "scaledBetaRecent":scaledBetaRecent,
        "scaledSharpeRecent":scaledSharpeRecent,
        "availableAlphaScaled":availableAlphaScaled,
        "availableBetaScaled":availableBetaScaled,
        "availableSharpeScaled":availableSharpeScaled,
        "availableReturnScaled":availableReturnScaled,
        "algoVsBenchmarkColsAvailableScaled":json.dumps(algoVsBenchmarkColsAvailableScaled),
        "algoVsBenchmarkRowsAvailableScaled":json.dumps(algoVsBenchmarkRowsAvailableScaled), 
    }
    


import pickle
from google.cloud import datastore, storage, logging

#AVAILABLE_MODE = "AVAILABLE"
#PAPER_TRADING_MODE = "PAPER_TRADING_MODE"
#TRADING_MODE = "TRADING_MODE"


def cachePortfolio(portfolioInfo, portfolioData, mode):
    lookupDB1 = None
    lookupDB2 = None
    if mode == params.AVAILABLE_MODE:
        lookupDB1 = params.portfolioDataCache
        lookupDB2 = params.portfolioQuickCache

    if mode == params.PAPER_TRADING_MODE:
        lookupDB1 = params.portfolioDataPaperTradingCache
        lookupDB2 = params.portfolioQuickPaperTradingCache
    
    if mode == params.TRADING_MODE:
        lookupDB1 = params.portfolioDataTradingCache
        lookupDB2 = params.portfolioQuickTradingCache

    portfolioHash = portfolioInfo["key"]
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(lookupDB1)
            blob = storage.Blob(portfolioHash, bucket)
            blob.upload_from_string(pickle.dumps(portfolioData))
            break
        except:
            print("UPLOAD BLOB ERROR:", str(sys.exc_info()))
            time.sleep(10)
    ##CACHE STATS
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            toUpload = {
                "benchmark":portfolioInfo["benchmark"],
                "description":portfolioInfo["description"],
                "portfolioType":portfolioInfo["portfolioType"],
                "startedTrading":portfolioInfo["startedTrading"]
                
            }
            lookups =  [
                "algoSharpe",
                "alpha",
                "beta",
                "annualReturn",
                "annualVolatility",
                "recentSharpe",
                "recentReturn",
                "recentAlpha",
                "recentBeta",
                "availableSharpe",
                "availableReturn",
                "availableAlpha",
                "availableBeta",
                "scaledReturn",
                "scaledSharpe",
                "scaledVolatility",
                "scaledAlpha",
                "scaledBeta",
                "scaledReturnRecent",
                "scaledVolatilityRecent",
                "scaledAlphaRecent",
                "scaledBetaRecent",
                "scaledSharpeRecent",
                "availableAlphaScaled",
                "availableBetaScaled",
                "availableSharpeScaled",
                "availableReturnScaled",
            ]
            for item in lookups:
                toUpload[item] = portfolioData[item]
            key = datastoreClient.key(lookupDB2, portfolioHash) #NEED TO HASH TO ENSURE NON-OVERLAPPING PREDICTIONS
            organismToStore = datastore.Entity(key=key)
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)
    pass

def fetchPortfolio(portfolioHash, mode):
    lookupDB = None
    if mode == params.AVAILABLE_MODE:
        lookupDB = params.portfolioDataCache

    if mode == params.PAPER_TRADING_MODE:
        lookupDB = params.portfolioDataPaperTradingCache
    
    if mode == params.TRADING_MODE:
        lookupDB = params.portfolioDataTradingCache

    storageClient = storage.Client('money-maker-1236')
    failures = 0
    while True:
        try:
            bucket = storageClient.get_bucket(lookupDB)
            blob = storage.Blob(portfolioHash, bucket)
            return pickle.loads(blob.download_as_string())
            break
        except:
            print("DOWNLOAD BLOB ERROR:", str(sys.exc_info()))
            failures += 1
            if failures > 5:
                return None
            # time.sleep(10)
    pass

def fetchQuickPortfolios(mode):
    lookupDB = None
    if mode == params.AVAILABLE_MODE:
        lookupDB = params.portfolioQuickCache

    if mode == params.PAPER_TRADING_MODE:
        lookupDB = params.portfolioQuickPaperTradingCache
    
    if mode == params.TRADING_MODE:
        lookupDB = params.portfolioQuickTradingCache

    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=lookupDB)
            retrievedPortfolios = []
            for item in list(query.fetch()):
                toSave = {
                    "key":item.key.name
                }
                lookups =  [
                    "algoSharpe",
                    "alpha",
                    "beta",
                    "annualReturn",
                    "annualVolatility",
                    "recentSharpe",
                    "recentReturn",
                    "recentAlpha",
                    "recentBeta",
                    "availableSharpe",
                    "availableReturn",
                    "availableAlpha",
                    "availableBeta",
                    "scaledReturn",
                    "scaledSharpe",
                    "scaledVolatility",
                    "scaledAlpha",
                    "scaledBeta",
                    "scaledReturnRecent",
                    "scaledVolatilityRecent",
                    "scaledAlphaRecent",
                    "scaledBetaRecent",
                    "scaledSharpeRecent",
                    "availableAlphaScaled",
                    "availableBetaScaled",
                    "availableSharpeScaled",
                    "availableReturnScaled",
                    "benchmark",
                    "description",
                    "portfolioType",
                    "startedTrading"
                ]
                for key in lookups:
                    toSave[key] = item[key]
                retrievedPortfolios.append(toSave)

            return retrievedPortfolios

        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            
def fetchPortfolioInfo(portfolioHash, mode):
    lookupDB = None
    if mode == params.AVAILABLE_MODE:
        lookupDB = params.portfolioQuickCache

    if mode == params.PAPER_TRADING_MODE:
        lookupDB = params.portfolioQuickPaperTradingCache
    
    if mode == params.TRADING_MODE:
        lookupDB = params.portfolioQuickTradingCache

    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            key = datastore_client.key(lookupDB, portfolioHash)
            item = datastore_client.get(key)
            retrievedPortfolio = {
                "key":item.key.name,
                "description":item["description"],
                "benchmark":item["benchmark"],
                "portfolioType":item["portfolioType"],
                "algoSharpe":item["algoSharpe"],
                "alpha":item["alpha"] * 100,
                "beta":item["beta"],
                "annualReturn":item["annualReturn"] * 100,
                "annualVolatility":item["annualVolatility"] * 100,
                "recentSharpe":item["recentSharpe"],
                "recentReturn":item["recentReturn"] * 100,
                "recentAlpha":item["recentAlpha"] * 100,
                "recentBeta":item["recentBeta"],
                "availableSharpe":item["availableSharpe"],
                "availableReturn":item["availableReturn"] * 100 if item["availableReturn"] != "NaN" else "NaN",
                "availableAlpha":item["availableAlpha"] * 100 if item["availableAlpha"] != "NaN" else "NaN",
                "availableBeta":item["availableBeta"],
                "startedTrading":item["startedTrading"]
            }

            return retrievedPortfolio
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))


def getTradingPortfolioHashes(mode, includeDates = False):


    lookupDB = None
    if mode == params.AVAILABLE_MODE:
        lookupDB = params.portfolioLookup

    if mode == params.PAPER_TRADING_MODE:
        lookupDB = params.paperTradingPortfolios
    
    if mode == params.TRADING_MODE:
        lookupDB = params.tradingPortfolios

    ##APPLICABLE DBs
    #params.paperTradingPortfolios
    #params.tradingPortfolios
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=lookupDB)
            fetchedData = list(query.fetch())
            retrievedPortfolios = [item.key.name for item in fetchedData]

            if includeDates == True:
                retrievedPortfolios = {}
                for item in fetchedData:
                    retrievedPortfolios[item.key.name] = item["startedTrading"]

            return retrievedPortfolios
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def getFundData():
    storageClient = storage.Client('money-maker-1236')
    historicalData = None
    realizedData = None
    try:
        bucket = storageClient.get_bucket(params.portfolioDataTradingCache)
        blob = storage.Blob("HISTORICALFUND", bucket)
        historicalData = pickle.loads(blob.download_as_string())
    except:
        print("DOWNLOAD BLOB ERROR:", str(sys.exc_info()))

    try:
        bucket = storageClient.get_bucket(params.portfolioDataTradingCache)
        blob = storage.Blob("REALIZEDFUND", bucket)
        realizedData = pickle.loads(blob.download_as_string())
    except:
        print("DOWNLOAD BLOB ERROR:", str(sys.exc_info()))
    return historicalData, realizedData



def estimateTransactionCost(allocationsTable):
    commissionPerTransaction = 0.002
    allocationChanges = allocationsTable.diff(1)
    allocationChanges[0:1] = allocationsTable[0:1] ##ABSORB FULL CHANGE ON FIRST DAY
    allocationChanges = allocationChanges.apply(lambda x:[abs(item * commissionPerTransaction) for item in x], axis=1)
    return allocationChanges

def calculatePerformanceForAllocations(allocations, joinedData):
    rawAllocationPerformance = calculatePerformanceForTable(allocations, allocations.columns, joinedData)
    estimatedTransactionCost = estimateTransactionCost(allocations)
    allocationPerformance = rawAllocationPerformance - estimatedTransactionCost
    return allocationPerformance.dropna(),\
        pd.DataFrame(allocationPerformance.apply(lambda x: sum(x), axis=1), columns = ["Fund Return"]).dropna(),\
        pd.DataFrame(estimatedTransactionCost.apply(lambda x: sum(x), axis=1), columns=["Fund Transaction Cost"]).dropna()


##STORE AND RETRIEVE SYSTEM UPDATE STATUS

def storeSystemStatus(dayToLog, startTime, endTime, elapsedTime, modelsSeen, portfoliosSeen):
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            key = datastoreClient.key(params.systemStatus)
            organismToStore = datastore.Entity(key=key)
            toUpload = {
                "day":dayToLog,
                "startTime":startTime,
                "endTime":endTime,
                "elapsedTime":elapsedTime,
                "modelsSeen":modelsSeen,
                "portfoliosSeen":portfoliosSeen
            }
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)
    pass
    

def getSystemStatus(dayToCheck = None):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.systemStatus, order=["-day"])
            if dayToCheck is not None:
                query.add_filter('day', '=', dayToCheck)
            
            retrievedUpdates = list(query.fetch())

            return retrievedUpdates
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))


