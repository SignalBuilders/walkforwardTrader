import dataAck
import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import numpy as np
import pandas as pd
def getModels(ticker = None, returnEntireObject = False):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.datastoreName)
            if ticker is not None:
                query.add_filter('ticker', '=', ticker)
            retrievedModels = list(query.fetch())
            toReturn = []
            for source in retrievedModels:
                if returnEntireObject == False:
                    toReturn.append(pickle.loads(source["model"]))
                else:
                    source["model"] = pickle.loads(source["model"])
                    toReturn.append(source)
            return toReturn
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))


def generateAggregateReturns(allModels, joinedData):
    aggregateReturns = None
    for mod in allModels:
        print(mod.describe())
        algoReturn, factorReturn, predictions =  mod.makePredictions(joinedData, 300) ##ONLY GET LAST 300 PREDICTIONS
#         print("TRAIN:", vizResults(algoReturn[:-252], factorReturn[:-252], True))
#         print("TEST:", vizResults(algoReturn[-252:], factorReturn[-252:], True))
        algoReturn.columns = [str(mod.describe())]
        if aggregateReturns is None:
            aggregateReturns = algoReturn
        else:
            aggregateReturns = aggregateReturns.join(algoReturn)
    return aggregateReturns



##USE HRP
import hrpPortfolioOpt as hrp

##GENERATE EACH ALGO RETURN
def simpleTransform(inputArr):
    toReturn = []
    for item in inputArr:
        if item > 0.5:
            toReturn.append(1.0)
        elif item < 0.5:
            toReturn.append(-1.0)
        else:
            toReturn.append(0.0)
    return toReturn
def produceHRPPredictions(aggregateReturns, windowSize, maxWindowSize = False):
    hrpReturns = pd.DataFrame([])
    historicalWeights = pd.DataFrame([])
    i = windowSize
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
        i += 1
    return hrpReturns, weights


import params
def storeModelPrediction(model, pred, lastDataDayUsed, shouldReturn = False):
    toUpload = {}
    toUpload["ticker"] = model.inputSeries.targetTicker
    toUpload["predictionLength"] = model.inputSeries.predictionPeriod
    toUpload["model"] = str(model.describe())
    toUpload["prediction"] = pred
    toUpload["lastDataDayUsed"] = lastDataDayUsed
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            predictionHash = hashlib.sha224((str(model.describe()) + " " + str(toUpload["lastDataDayUsed"])).encode('utf-8')).hexdigest()
            key = datastoreClient.key(params.predictionsName, predictionHash) #NEED TO HASH TO ENSURE NON-OVERLAPPING PREDICTIONS
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


def getModelPrediction(ticker = None):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.predictionsName)
            if ticker is not None:
                query.add_filter('ticker', '=', ticker)
            retrievedPredictions = list(query.fetch())
            toReturn = []
            for pred in retrievedPredictions:
                toReturn.append(pred)
            return toReturn
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def getTickersWithModel():
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.datastoreName, projection=["ticker"], distinct_on=["ticker"])
            retrievedDatasources = list(query.fetch())
            toReturn = {}
            for source in retrievedDatasources:
                toReturn[source["ticker"]] = source["ticker"]

            return [item for item in toReturn]
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def getModelsByKey(modelHashes):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            ##form keys
            keys = []
            for hashing in modelHashes:
                key = datastore_client.key(params.datastoreName, hashing)
                keys.append(key)
                
            retrievedModels = datastore_client.get_multi(keys)
            toReturn = []
            for source in retrievedModels:
                toReturn.append(pickle.loads(source["model"]))
            return toReturn
            
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            
def getPortfolioByKey(portfolioHash):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            ##form keys
            key = datastore_client.key(params.portfolioLookup, portfolioHash)
                
            return datastore_client.get(key)
            
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            
def getPortfolios():
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.portfolioLookup)
            retrievedPortfolios = [{
                "key":item.key.name,
                "description":item["description"],
                "benchmark":item["benchmark"]
            } for item in list(query.fetch())]

            return retrievedPortfolios
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

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

def storeAggregateModelPrediction(model, pred, predictionDay, shouldReturn = False):
    ##STORES AGGREGATE PREDICTION MADE ON A GIVEN DAY
    toUpload = {}
    toUpload["ticker"] = model.inputSeries.targetTicker
    toUpload["aggregatePrediction"] = pred
    toUpload["predictionDay"] = predictionDay
    toUpload["modelHash"] = hashlib.sha224((str(model.describe())).encode('utf-8')).hexdigest()
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            predictionHash = hashlib.sha224((str(model.describe()) + " " + str(toUpload["predictionDay"])).encode('utf-8')).hexdigest()
            key = datastoreClient.key(params.aggregatePrediction, predictionHash) #NEED TO HASH TO ENSURE NON-OVERLAPPING PREDICTIONS
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
            
            
def getPredictionsByModel(model):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.predictionsName)
            query.add_filter('model', '=', str(model.describe()))
            retrievedPredictions = list(query.fetch())
            toReturn = []
            for pred in retrievedPredictions:
                toReturn.append(pred)
            return toReturn
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
    

def getAggregatePredictionForModelDaily(model, joinedData):
    todayPredictions = []
    for pred in getPredictionsByModel(model):
        ##CHECK IF PREDICTION STILL VALID
        if len(joinedData[str(pred["lastDataDayUsed"]):]) - 1 < pred["predictionLength"]:##GETS TRADING DAYS SINCE LAST DATA DAY
            todayPredictions.append(pred["prediction"])
    #print(model.describe(), todayPredictions, dataAck.computePosition(todayPredictions))
    return dataAck.computePosition(todayPredictions)

import datetime
import pytz
def getToday():
    dt = datetime.datetime.utcnow()
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt

def storeManyItems(items):
    ##UPLOAD ORGANISM OBJECT
    i = 0
    while i < len(items):
        while True:
            try:
                datastoreClient = datastore.Client('money-maker-1236')
                datastoreClient.put_multi(items[i:i+200])
                break
            except:
                print("UPLOAD ERROR:", str(sys.exc_info()))
                time.sleep(10)
        i += 200
        
def getPortfolioAllocations(portfolioKey, predictionDay = None):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.portfolioAllocation)
            query.add_filter('portfolio', '=', portfolioKey)
            if predictionDay is not None:
                query.add_filter('predictionDay', '=', predictionDay)
            retrievedPredictions = list(query.fetch())
            return retrievedPredictions
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

import hashlib
def getModelHash(model):
    return hashlib.sha224((str(model.describe())).encode('utf-8')).hexdigest()
