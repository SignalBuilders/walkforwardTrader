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



def generateAllReturnsFromCache(allModels):
    aggregateReturns = None
    aggregatePredictions = None
    aggregateSlippageReturns = None
    cleanedModels = []
    for mod in allModels:
        
        try:
            algoReturn, algoPredictions, algoSlippageAdjustedReturn = dataAck.getModelData(mod)
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
        except:
            print("SKIPPING", mod.describe())
    return aggregateReturns, aggregatePredictions, aggregateSlippageReturns, cleanedModels

def computeReturnsForUniqueModelsCache(uniqueModels, factorToTrade):
    tickersRequired = []
    for mod in uniqueModels:

        print(mod.describe())
        if mod.inputSeries.targetTicker not in tickersRequired:
            tickersRequired.append(mod.inputSeries.targetTicker)
        if mod.inputSeries.series.ticker not in tickersRequired:
            tickersRequired.append(mod.inputSeries.series.ticker)
            
    if factorToTrade not in tickersRequired:
        tickersRequired.append(factorToTrade)
    
    pulledData, validTickers = dataAck.downloadTickerData(tickersRequired)

    joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])
    
    modelReturns, modelPredictions, modelSlippageReturns, cleanedModels = generateAllReturnsFromCache(uniqueModels)
    
    return cleanedModels, modelReturns, modelPredictions, modelSlippageReturns, modelReturns.join(dataAck.getDailyFactorReturn(factorToTrade, joinedData)).dropna(), joinedData









from google.cloud import datastore, storage, logging
import time
import params
import hashlib
def checkAggregatePredictionsStored(model):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.aggregatePrediction)
            
            query.add_filter('modelHash', '=', hashlib.sha224((str(model.describe())).encode('utf-8')).hexdigest())
            retrievedPredictions = list(query.fetch(limit=1))
            if len(retrievedPredictions) > 0:
                return True
            else:
                return False
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            
            
            
            
            
def runModPredictionBackfill(mod, dataToUse, backfillDays = 30):
    ##ENSURE POPULATED FOR CORRECT PREDICTION STYLE
    pred = dataAck.computePosition([mod.makeTodayPrediction(dataToUse)])
    print(mod.describe(), pred, dataToUse.index[-1])
    portfolio.storeModelPrediction(mod, pred, dataToUse.index[-1])
    
def runBackfillMP(mod, joinedData, threadsToUse, backfillDays = 30):
    mpEngine = mp.get_context('fork')
    i = mod.inputSeries.predictionPeriod - 1 + backfillDays
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
    i = mod.inputSeries.predictionPeriod - 1 + backfillDays
    allPreds = portfolio.getPredictionsByModel(mod)
    while i > 0:
        lastDay = joinedData[:-i].index[-1]
        todayPredictions = []
        for pred in allPreds:
            ##CHECK IF PREDICTION STILL VALID
            if len(joinedData[str(pred["lastDataDayUsed"]):lastDay]) - 1 < pred["predictionLength"] and len(joinedData[str(pred["lastDataDayUsed"]):lastDay]) > 0:##GETS TRADING DAYS SINCE LAST DATA DAY
                todayPredictions.append(pred["prediction"])
       
        ##SKIP UPLOAD IF NOT ENOUGH PREDICTIONS
        print(lastDay, len(todayPredictions))
        if len(todayPredictions) == mod.inputSeries.predictionPeriod:
            pred = dataAck.computePosition(todayPredictions)
            print(mod.describe(), todayPredictions, pred)
            portfolio.storeAggregateModelPrediction(mod, pred, lastDay)
        i -= 1
    
    
    
def returnSelectAlgos(algoColumns):
    return np.random.choice(algoColumns, size=random.randint(7, len(algoColumns)), replace= False)

# import matplotlib.pyplot as plt
import time
import random


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
        print(thisModel.describe())
        if thisModel.inputSeries.targetTicker not in tickersSeen:
            tickersSeen[thisModel.inputSeries.targetTicker] = 0
        tickersSeen[thisModel.inputSeries.targetTicker] += 1
    
    ##STORE MODEL
    portfolioHash = portfolioGeneration.storePortfolio(modelsUsed,\
            description=str(tickersSeen), benchmark="SPY", portfolioType="HRP FULL")
    
    portfolioGeneration.storeHistoricalAllocations(portfolioHash, \
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
    portfolioData = portfolioGeneration.getDataForPortfolio(portfolioHash, factorToTrade, joinedData, portfolioInfo["startedTrading"])
    portfolioGeneration.cachePortfolio(portfolioInfo, portfolioData, params.AVAILABLE_MODE)

    
    
## MP RUN            
def createPossiblePortfoliosMP(cleanedPredictions, cleanedReturns, hashToModel, joinedData, threadsToUse):
    mpEngine = mp.get_context('fork')
        
    runningP = []
    while True:
        selectedAlgorithms = returnSelectAlgos(cleanedReturns.columns)
        
        while len(runningP) > threadsToUse:
            runningP = dataAck.cycleP(runningP)
        
        p = mpEngine.Process(target=performPortfolioPerformanceEstimation, args=(cleanedPredictions[selectedAlgorithms], cleanedReturns[selectedAlgorithms], hashToModel, joinedData, ))
        p.start()
        runningP.append(p)
        
    
    
    
   