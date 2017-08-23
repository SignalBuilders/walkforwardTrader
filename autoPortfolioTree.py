from google.cloud import datastore, storage, logging
import time
import params
import hashlib
import curveTreeDB
import portfolioGeneration
import dataAck
import multiprocessing as mp
import pandas as pd
import numpy as np
import portfolio

def generateAllReturnsFromCache(allModels):
    aggregateReturns = None
    aggregatePredictions = None
    aggregateSlippageReturns = None
    cleanedModels = []
    for mod in allModels:
        
        # try:
        algoReturn, algoPredictions, algoSlippageAdjustedReturn = curveTreeDB.getModelData(params.treeModelData, mod)
        print(mod.describe())
        algoReturn.columns = [str(mod.describe())]
        algoPredictions.columns = [str(mod.describe())]
        algoSlippageAdjustedReturn.columns =  [str(mod.describe())]
        if aggregateReturns is None:
            aggregateReturns = algoReturn
            aggregatePredictions = algoPredictions
            aggregateSlippageReturns = algoSlippageAdjustedReturn
        else:
            aggregateReturns = aggregateReturns.join(algoReturn)
            aggregatePredictions = aggregatePredictions.join(algoPredictions)
            aggregateSlippageReturns = aggregateSlippageReturns.join(algoSlippageAdjustedReturn)
        cleanedModels.append(mod)
        # except:
        #     print("SKIPPING", mod.describe())
    return aggregateReturns, aggregatePredictions, aggregateSlippageReturns, cleanedModels


def computeReturnsForUniqueModelsCache(uniqueModels, factorToTrade):
    tickersRequired = []
    for mod in uniqueModels:

        print(mod.describe())
        for ticker in mod.returnAllTickersInvolved():
            if ticker not in tickersRequired:
                tickersRequired.append(ticker)

    if factorToTrade not in tickersRequired:
        tickersRequired.append(factorToTrade)
            
    
    pulledData, validTickers = dataAck.downloadTickerData(tickersRequired)

    joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])
    
    modelReturns, modelPredictions, modelSlippageReturns, cleanedModels = generateAllReturnsFromCache(uniqueModels)
    
    return cleanedModels, modelReturns, modelPredictions, modelSlippageReturns, modelReturns.join(dataAck.getDailyFactorReturn(factorToTrade, joinedData)).dropna(), joinedData


def checkAggregatePredictionsStored(model):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.aggregatePrediction)
            
            query.add_filter('modelHash', '=', model.getHash())
            retrievedPredictions = list(query.fetch(limit=1))
            if len(retrievedPredictions) > 0:
                return True
            else:
                return False
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            
            
            
def storePortfolio(models, description, benchmark, portfolioType):
    allHashes = []
    for model in models:
        organismHash = hashlib.sha224(model.getHash().encode('utf-8')).hexdigest()
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
                "startedTrading":curveTreeDB.getToday()
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
            
def runModPredictionBackfill(mod, dataToUse, backfillDays = 30):
    ##ENSURE POPULATED FOR CORRECT PREDICTION STYLE
    pred = mod.runModelToday(dataToUse)
    print(mod.describe(), pred, dataToUse.index[-1])
    curveTreeDB.storeModelPrediction(mod, pred, dataToUse.index[-1])
    
def runBackfillMP(mod, joinedData, threadsToUse, backfillDays = 30):
    mpEngine = mp.get_context('fork')
    i = mod.predictionDistance - 1 + backfillDays
    runningP = []
    while i > 0:
        while len(runningP) > threadsToUse:
            runningP = dataAck.cycleP(runningP)
        
        p = mpEngine.Process(target=runModPredictionBackfill, args=(mod, joinedData[:-i], backfillDays, ))
        p.start()
        runningP.append(p)
        i -= 1
    
    while len(runningP) > 0:
        runningP = dataAck.cycleP(runningP)
    
    print("CHECKING AGGREGATE PREDICTIONS")
    ##STORE AGGREGATE PREDICTIONS
    i = mod.predictionDistance - 1 + backfillDays
    allPreds = curveTreeDB.getPredictionsByModel(mod)
    while i > 0:
        lastDay = joinedData[:-i].index[-1]
        todayPredictions = []
        for pred in allPreds:
            ##CHECK IF PREDICTION STILL VALID
            if len(joinedData[str(pred["lastDataDayUsed"]):lastDay]) - 1 < pred["predictionLength"] and len(joinedData[str(pred["lastDataDayUsed"]):lastDay]) > 0:##GETS TRADING DAYS SINCE LAST DATA DAY
                todayPredictions.append(pred["prediction"])
       
        ##SKIP UPLOAD IF NOT ENOUGH PREDICTIONS
        print(lastDay, len(todayPredictions))
        if len(todayPredictions) == mod.predictionDistance:
            pred = dataAck.computePosition(todayPredictions)
            print(mod.describe(), todayPredictions, pred)
            curveTreeDB.storeAggregateModelPrediction(mod, pred, lastDay)
        i -= 1

def downloadAggregatePredictions(model):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.aggregatePrediction)
            
            query.add_filter('modelHash', '=', model.getHash())
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
        dailyFactorReturn = dataAck.getDailyFactorReturn(model.targetTicker, joinedData)
        transformedPreds = preds.join(dailyFactorReturn).dropna()
        returnStream = pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=[model.getHash()])
        preds.columns = [model.getHash()]
        if aggregateReturns is None:
            aggregateReturns = returnStream
            aggregatePredictions = preds
        else:
            aggregateReturns = aggregateReturns.join(returnStream)
            aggregatePredictions = aggregatePredictions.join(preds)
    return aggregateReturns, aggregatePredictions
    
    
def returnSelectAlgos(algoColumns):
    return np.random.choice(algoColumns, size=random.randint(2, len(algoColumns)), replace= False)

# import matplotlib.pyplot as plt
import time
import random

def storeHistoricalAllocations(portfolioKey, modelsInPortfolio, historicalWeights, aggregatePredictions):

    aggregatePredictions = aggregatePredictions.dropna()
    allocationsToStore = []
    ##ITERATE THROUGH DAYS TO CALCULATE NET POSITION
    for i in range(len(historicalWeights)):
        netPosition = {}
        weights = historicalWeights.iloc[i]
        transformedAlgoPrediction = {}
        for model in modelsInPortfolio:
            if model.targetTicker not in netPosition:
                netPosition[model.targetTicker] = 0.0
            try:
                aggregatePredictions.loc[historicalWeights.index[i]]
            except:
                continue
            
            netPosition[model.targetTicker] += weights[model.getHash()] * aggregatePredictions.loc[historicalWeights.index[i]][model.getHash()]
            transformedAlgoPrediction[model.getHash()] = weights[model.getHash()] * aggregatePredictions.loc[historicalWeights.index[i]][model.getHash()]
        allocationsToStore.append(portfolioGeneration.storePortfolioAllocation(portfolioKey, historicalWeights.index[i], weights.to_dict(), netPosition, transformedAlgoPrediction, shouldReturn=True))
    curveTreeDB.storeManyItems(allocationsToStore)


def performPortfolioPerformanceEstimation(thisPredictions, thisReturns, hashToModel, joinedData):
    hrpReturns, historicalWeights = portfolioGeneration.\
            produceHRPPredictions(thisReturns,\
            126, startIndex=None, maxWindowSize=False)
    print("COMPUTED HISTORICAL WEIGHTS")
    
    modelsUsed = []
    
    tickersSeen = {}
    
    for modelHash in thisPredictions.columns:
        thisModel = hashToModel[modelHash]
        modelsUsed.append(thisModel)
        print(thisModel.describe(), thisModel.getHash())
        if thisModel.targetTicker not in tickersSeen:
            tickersSeen[thisModel.targetTicker] = 0
        tickersSeen[thisModel.targetTicker] += 1
    
    ##STORE MODEL
    portfolioHash = storePortfolio(modelsUsed,\
            description=str(tickersSeen), benchmark="SPY", portfolioType="HRP FULL")
    
    storeHistoricalAllocations(portfolioHash, \
                    modelsUsed, historicalWeights, thisPredictions)
    
    portfolioInfo = portfolio.getPortfolioByKey(portfolioHash)
    portfolioInfo = {
        "key":portfolioInfo.key.name,
        "description":portfolioInfo["description"],
        "benchmark":portfolioInfo["benchmark"],
        "portfolioType":portfolioInfo["portfolioType"],
        "startedTrading":portfolioInfo["startedTrading"]
    }
    print(portfolioInfo)
    portfolioData = getDataForPortfolio(portfolioHash, portfolioInfo["benchmark"], joinedData, portfolioInfo["startedTrading"])
    portfolioGeneration.cachePortfolio(portfolioInfo, portfolioData, params.AVAILABLE_MODE)

    
    
## MP RUN            
def createPossiblePortfoliosMP(cleanedPredictions, cleanedReturns, hashToModel, joinedData, threadsToUse):
    mpEngine = mp.get_context('fork')
        
    runningP = []
    while True:
        selectedAlgorithms = returnSelectAlgos(cleanedReturns.columns)
        print("SELECTED ALGOS",selectedAlgorithms)
        
        while len(runningP) > threadsToUse:
            runningP = dataAck.cycleP(runningP)
        
        p = mpEngine.Process(target=performPortfolioPerformanceEstimation, args=(cleanedPredictions[selectedAlgorithms], cleanedReturns[selectedAlgorithms], hashToModel, joinedData, ))
        p.start()
        runningP.append(p)
        
import json
import empyrical

def getModelsByKey(modelHashes):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            ##form keys
            keys = []
            for hashing in modelHashes:
                key = datastore_client.key(params.treeModels, hashing)
                keys.append(key)
                
            retrievedModels = datastore_client.get_multi(keys)
            toReturn = []
            for source in retrievedModels:
                toReturn.append(pickle.loads(source["model"]))
            return toReturn
            
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def getDataForPortfolio(portfolioKey, factorToTrade, joinedData, availableStartDate):
    modelHashes = portfolio.getPortfolioModels(portfolioKey)
    print("MODEL HASHES", modelHashes)
    models = getModelsByKey(modelHashes)
    print("MODELS FOR PORTFOLIO", models)
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
    
    rawTickerPerformance = portfolioGeneration.calculatePerformanceForTable(tickerAllocationsTable, tickerAllocationsTable.columns, joinedData)
    
    rawAlgoPerformance = pd.DataFrame(rawTickerPerformance.apply(lambda x:sum(x), axis=1), columns=["Algo Return Without Commissions"])
    
    tickerPerformance, algoPerformance, algoTransactionCost =  portfolioGeneration.calculatePerformanceForAllocations(tickerAllocationsTable, joinedData)
    
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
    rawTickerPerformanceScaled = portfolioGeneration.calculatePerformanceForTable(scaledTickerAllocationsTable, scaledTickerAllocationsTable.columns, joinedData)
    
    rawAlgoPerformanceScaled = pd.DataFrame(rawTickerPerformanceScaled.apply(lambda x:sum(x), axis=1), columns=["Algo Return Without Commissions"])
    
    unused, algoPerformanceScaled, algoTransactionCostScaled =  portfolioGeneration.calculatePerformanceForAllocations(scaledTickerAllocationsTable, joinedData)
    

    algoPerformanceScaled.columns = ["Algo Return"]
    algoVsBenchmarkScaled = algoPerformanceScaled.join(factorReturn).dropna()
    algoVsBenchmarkScaled = algoVsBenchmarkScaled.join(rawAlgoPerformanceScaled).dropna()
    
    
    
    ##FORM HASH TO TICKER
    hashToTicker = {}
    for model in models:
        hashToTicker[model.getHash()] = model.targetTicker
    print(hashToTicker)

    individualAlgoPerformance = portfolioGeneration.calculatePerformanceForTable(predsTable,[hashToTicker[modelHash] for modelHash in predsTable.columns], joinedData)
    
    ##CONVERT TO USABLE OBJECTS
    tickerCols, tickerRows = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(tickerPerformance))
    tickerAllocationsCols, tickerAllocationsRows = portfolioGeneration.convertTableToJSON(tickerAllocationsTable[-10:])
    algoCols, algoRows = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(algoPerformance))
    algoVsBenchmarkCols, algoVsBenchmarkRows = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(algoVsBenchmark))
    individualAlgoPerformanceCols, individualAlgoPerformanceRows = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(individualAlgoPerformance))
    scaledAllocationCols, scaledAllocationRows = portfolioGeneration.convertTableToJSON(scaledTickerAllocationsTable)
    weightsCols, weightsRows = portfolioGeneration.convertTableToJSON(weightsTable)
    alpha, beta = empyrical.alpha_beta(algoPerformance, factorReturn)
    recentAlpha, recentBeta = empyrical.alpha_beta(algoPerformance[-100:], factorReturn[-100:])
    recentSharpe = empyrical.sharpe_ratio(algoPerformance[-100:])
    recentReturn = empyrical.cum_returns(algoPerformance[-100:]).values[-1][0] * 100
    algoVsBenchmarkColsRecent, algoVsBenchmarkRowsRecent = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(algoVsBenchmark[-100:]))
    commissionCols, commissionRows = portfolioGeneration.convertTableToJSON(algoTransactionCost)
    
    algoVsBenchmarkScaledCols, algoVsBenchmarkScaledRows = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(algoVsBenchmarkScaled))
    commissionScaledCols, commissionScaledRows = portfolioGeneration.convertTableToJSON(algoTransactionCostScaled)
    scaledSharpe = empyrical.sharpe_ratio(algoPerformanceScaled)
    scaledReturn = empyrical.annual_return(algoPerformanceScaled)[0] * 100
    scaledVolatility = empyrical.annual_volatility(algoPerformanceScaled) * 100
    scaledAlpha, scaledBeta = empyrical.alpha_beta(algoPerformanceScaled, factorReturn)
    
    
    algoVsBenchmarkScaledColsRecent, algoVsBenchmarkScaledRowsRecent = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(algoVsBenchmarkScaled[-100:]))
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
        algoVsBenchmarkColsAvailable, algoVsBenchmarkRowsAvailable = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(algoVsBenchmark[availableStartDate:]))
        
        ##SCALED
        availableAlphaScaled, availableBetaScaled = empyrical.alpha_beta(algoPerformanceScaled[availableStartDate:], factorReturn[availableStartDate:])
        availableAlphaScaled = availableAlphaScaled * 100
        availableSharpeScaled = empyrical.sharpe_ratio(algoPerformanceScaled[availableStartDate:])
        availableReturnScaled = empyrical.cum_returns(algoPerformanceScaled[availableStartDate:]).values[-1][0] * 100
        algoVsBenchmarkColsAvailableScaled, algoVsBenchmarkRowsAvailableScaled = portfolioGeneration.convertTableToJSON(empyrical.cum_returns(algoVsBenchmarkScaled[availableStartDate:]))
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
    
    
    
   