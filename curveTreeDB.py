import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import dataAck


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
            predictionHash = hashlib.sha224(model.getHash() + " " + str(toUpload["lastDataDayUsed"])).encode('utf-8')).hexdigest()
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


