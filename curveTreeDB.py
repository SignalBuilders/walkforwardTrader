import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import dataAck
import multiprocessing as mp


##USED TO STORE DESCRIPTION INFO
def storeModel(db, model, uploadInformation, trainingMetrics, oosMetrics):
    toUpload = uploadInformation
    for k in trainingMetrics:
        toUpload["IS_" + k] = trainingMetrics[k]
    for k in oosMetrics:
        toUpload["OOS_" + k] = oosMetrics[k]
    toUpload["model"] = pickle.dumps(model)
    organismHash = model.getHash()
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            key = datastoreClient.key(db,  organismHash) #NEED TO HASH TO ENSURE UNDER COUNT
            organismToStore = datastore.Entity(key=key, exclude_from_indexes=["model"])
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)
    
    ##LOG SUCCESSFUL STORE
    toLog = {}
    for item in toUpload:
        if item != "model":
            toLog[item] = toUpload[item]
        else:
            toLog[item] = str(model.describe())
    dataAck.logModel("StoredModel"+"_" + db, toLog)

def getModels(db, ticker = None, returnEntireObject = False):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=db)
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

def getValidModels(db, returnEntireObject = False):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=db)
            query.add_filter("IS_SHARPE DIFFERENCE SLIPPAGE", '>', 0.0)
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


##USED TO STORE PREDICTIONS -> USEFUL FOR PORTFOLIO CONSTRUCTION...ONLY DO FOR TREE PREDICTOR
def storeModelData(db, model, algoReturns, algoPredictions, algoReturnsSlippage):
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(db)
            organismHash = model.getHash()
            blob = storage.Blob(organismHash, bucket)
            blob.upload_from_string(pickle.dumps((algoReturns, algoPredictions, algoReturnsSlippage)))
            print("STORING", organismHash)
            break
        except:
            print("UPLOAD BLOB ERROR:", str(sys.exc_info()))
            time.sleep(10)
    pass

def getModelData(db, model):
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(db)
            organismHash = model.getHash()
            print("ATTEMPTING PULL", organismHash)
            blob = storage.Blob(organismHash, bucket)
            return pickle.loads(blob.download_as_string())
        except:
            return None
    pass

def storeModelPrediction(model, pred, lastDataDayUsed, shouldReturn = False):
    toUpload = {}
    toUpload["ticker"] = model.targetTicker
    toUpload["predictionLength"] = model.predictionDistance
    toUpload["model"] = model.getHash()
    toUpload["prediction"] = pred
    toUpload["lastDataDayUsed"] = lastDataDayUsed
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            predictionHash = hashlib.sha224((model.getHash() + " " + str(toUpload["lastDataDayUsed"])).encode('utf-8')).hexdigest()
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

def storePastPredictions(allModels, modelPredictions):
    ##THESE ARE SUMMED PREDICTIONS...DIFFERENT THAN PREDICTIONS MADE DAILY
    print("LENGTH OF PREDICTIONS TO STORE", len(modelPredictions))
    lastDayUsedPredictions = modelPredictions.dropna()
    allStoredModels = getAllPortfolioModels()
    print(allStoredModels)
    for i in range(len(lastDayUsedPredictions.columns)):
        ##CHECK IF ALREADY STORED
        
        thisModel = allModels[i]
        if thisModel.getHash() in allStoredModels:
            print("SKIPPING", thisModel.describe())
            continue
        
        print(thisModel.describe())
        thisDF = lastDayUsedPredictions[[lastDayUsedPredictions.columns[i]]]
        predictionsToStore = []
        for j in range(len(thisDF.values)):
#             print(thisDF.index[i], thisDF.values[i][0])
            predictionsToStore.append(storeAggregateModelPrediction(thisModel, thisDF.values[j][0], thisDF.index[j], shouldReturn=True))
        print("NEED TO STORE", len(predictionsToStore))
        storeManyItems(predictionsToStore)

def getPredictionsByModel(model):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.predictionsName)
            query.add_filter('model', '=', model.getHash())
            retrievedPredictions = list(query.fetch())
            toReturn = []
            for pred in retrievedPredictions:
                toReturn.append(pred)
            return toReturn
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
    


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

def storeAggregateModelPrediction(model, pred, predictionDay, shouldReturn = False):
    ##STORES AGGREGATE PREDICTION MADE ON A GIVEN DAY
    toUpload = {}
    toUpload["ticker"] = model.targetTicker
    toUpload["aggregatePrediction"] = pred
    toUpload["predictionDay"] = predictionDay
    toUpload["modelHash"] = model.getHash()
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            predictionHash = hashlib.sha224((model.getHash() + " " + str(toUpload["predictionDay"])).encode('utf-8')).hexdigest()
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
                datastoreClient.put_multi(items[i:i+300])
                break
            except:
                print("UPLOAD ERROR:", str(sys.exc_info()))
                time.sleep(10)
        i += 300

def getPertinentDataForModels(allModels):
    tickersRequired = []
    for mod in allModels:

        print(mod.describe())
        for ticker in mod.returnAllTickersInvolved():
            if ticker not in tickersRequired:
                tickersRequired.append(ticker)
        

    pulledData, validTickers = dataAck.downloadTickerData(tickersRequired)

    joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])
    return joinedData


# params.curveModels
# params.treeModels
def getModelCounts(db):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = None
            if db == params.treeModels:
                query = datastore_client.query(kind=db, projection=["ticker", "predictionLength", "numberOfPredictors"])
            else:
                query = datastore_client.query(kind=db, projection=["ticker", "predictionLength"])
            retrievedModels = list(query.fetch())
            tickerCount = {}
            predictionCount = {}
            numPredictors = {}
            for source in retrievedModels:
                if source["ticker"] not in tickerCount:
                    tickerCount[source["ticker"]] = 0
                    predictionCount[source["ticker"]] = {}
                    if db == params.treeModels:
                        numPredictors[source["ticker"]] = {}
                if source["predictionLength"] not in predictionCount[source["ticker"]]:
                    predictionCount[source["ticker"]][source["predictionLength"]] = 0
                if db == params.treeModels:
                    if source["numberOfPredictors"] not in numPredictors[source["ticker"]]:
                        numPredictors[source["ticker"]][source["numberOfPredictors"]] = 0
                tickerCount[source["ticker"]] += 1
                predictionCount[source["ticker"]][source["predictionLength"]] += 1
                if db == params.treeModels:
                    numPredictors[source["ticker"]][source["numberOfPredictors"]] += 1
            return len(retrievedModels), tickerCount, predictionCount, numPredictors
        except:
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            time.sleep(10)

# params.curveModels
# params.treeModels
def getModelPerformance(db):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = None
            query = datastore_client.query(kind=db, projection=["ticker", "IS_SHARPE DIFFERENCE", "IS_SHARPE DIFFERENCE SLIPPAGE"])
            retrievedModels = list(query.fetch())
            sharpeDifference = {}
            sharpeDifferenceSlippage = {}
            for source in retrievedModels:
                if source["ticker"] not in sharpeDifference:
                    sharpeDifference[source["ticker"]] = {">=0":0, "<0":0}
                    sharpeDifferenceSlippage[source["ticker"]] = {">=0":0, "<0":0}
                if source["IS_SHARPE DIFFERENCE"] < 0:
                    sharpeDifference[source["ticker"]]["<0"] += 1
                else:
                    sharpeDifference[source["ticker"]][">=0"] += 1

                if source["IS_SHARPE DIFFERENCE SLIPPAGE"] < 0:
                    sharpeDifferenceSlippage[source["ticker"]]["<0"] += 1
                else:
                    sharpeDifferenceSlippage[source["ticker"]][">=0"] += 1

            return sharpeDifference, sharpeDifferenceSlippage
        except:
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            time.sleep(10)



