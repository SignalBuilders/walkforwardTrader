from google.cloud import datastore, storage, logging
import empyrical
import params
import time
import pickle
import hashlib
import sys
import numpy as np

def addDataSourcesForTicker(dataSources):
    ##CONSTRUCT ORG OBJECTS
    datastoreClient = datastore.Client('money-maker-1236')
    orgsToStore = []
    for item in dataSources:
        ticker, applicable_ticker = item
        key = datastoreClient.key(params.dataSourcesDatastoreName, ticker + " " + applicable_ticker)
        toUpload = {
            "applicable_ticker":applicable_ticker,
            "ticker":ticker
        }
        organismToStore = datastore.Entity(key=key)
        organismToStore.update(toUpload)
        orgsToStore.append(organismToStore)
    
    splitArrays = np.array_split(np.array(orgsToStore), int(len(orgsToStore)/300))
    for split in splitArrays:
        while True:
            try:
                datastoreClient = datastore.Client('money-maker-1236')
                datastoreClient.put_multi(split.tolist())
                break
            except:
                time.sleep(10)
                print("DATA SOURCE UPLOAD ERROR:", str(sys.exc_info()))
            
##ROBINHOOD APIS
def getQuote(TICKER):
    import urllib.request
    import json
    robinhoodURL = "https://api.robinhood.com/quotes/" + TICKER + "/"
    priceInfo = json.loads(urllib.request.urlopen(robinhoodURL).read().decode('utf-8'))["last_trade_price"]
    return float(priceInfo)

def getName(TICKER):
    import urllib.request
    import json
    robinhoodURL = "https://api.robinhood.com/instruments/?symbol=" + TICKER
    priceInfo = json.loads(urllib.request.urlopen(robinhoodURL).read().decode('utf-8'))["results"][0]["name"]
    return priceInfo

def getVolume(TICKER):
    import urllib.request
    import json
    robinhoodURL = "https://api.robinhood.com/fundamentals/" + TICKER + "/"
    priceInfo = json.loads(urllib.request.urlopen(robinhoodURL).read().decode('utf-8'))["average_volume"]
    return float(priceInfo)

def getApplicableEtfs():
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            query = datastoreClient.query(kind="applicable_etf")
            query.add_filter("Trading Days", ">", 2500)
            retrievedETFs = [item["Symbol"].decode('utf-8') for item in list(query.fetch())]
            etfInfo = {}
            for etf in retrievedETFs:
                try:
                    etfInfo[etf] = getQuote(etf) * getVolume(etf)
                    time.sleep(1)
                    print(etf, etfInfo[etf])
                except:
                    print("SKIPPED", etf)
            return etfInfo
        except:
            print("APPLICABLE ETF ERROR:", str(sys.exc_info()))
            time.sleep(10)
            
##STORE ETF TRADING VOLUME
def addETFVolume(ticker, volume):
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            key = datastoreClient.key("etf_volume", ticker)
            toUpload = {
                "volume":volume,
                "ticker":ticker
            }
            organismToStore = datastore.Entity(key=key)
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            break
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            
def getValidETFVolumes(minVolume):
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            query = datastoreClient.query(kind="etf_volume")
            query.add_filter("volume", ">", float(minVolume))
            retrievedETFs = [item["ticker"] for item in list(query.fetch())]
            return retrievedETFs
        except:
            print("APPLICABLE ETF ERROR:", str(sys.exc_info()))
            time.sleep(10)

def getAllTickersPlain():
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.dataSourcesDatastoreName)
            retrievedDatasources = list(query.fetch())
            toReturn = {}
            for source in retrievedDatasources:
                toReturn[source["ticker"]] = source["ticker"]

            return [item for item in toReturn]
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def getDataSourcesForTicker(ticker):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.dataSourcesDatastoreName)
            query.add_filter('ticker', '=', ticker)
            retrievedDatasources = list(query.fetch())
            toReturn = []
            for source in retrievedDatasources:
                toReturn.append(source["applicable_ticker"])

            return toReturn
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def storeSwatch(ticker, swatch, performanceInformation, oosPerformanceInformation):
    toUpload = performanceInformation
    for k in oosPerformanceInformation:
        toUpload["OOS_" + k] = oosPerformanceInformation[k]
    toUpload["ticker"] = ticker
    toUpload["swatch"] = pickle.dumps(swatch)
    organismHash = hashlib.sha224(str(swatch.describe()).encode('utf-8')).hexdigest()
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            key = datastoreClient.key(datastoreName,  organismHash) #NEED TO HASH TO ENSURE UNDER COUNT
            organismToStore = datastore.Entity(key=key, exclude_from_indexes=["swatch"])
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)

def getSwatches(ticker):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=datastoreName)
            query.add_filter('ticker', '=', ticker)
            retrievedSwatches = list(query.fetch())
            toReturn = []
            for source in retrievedSwatches:
                toReturn.append(pickle.loads(source["swatch"]))
            return toReturn
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
        

