import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import time
import multiprocessing as mp
import sys
import dataAck

def getValidModelsByTicker(db, ticker, sharedDict):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=db, projection=["ticker", "IS_SHARPE DIFFERENCE SLIPPAGE"])
            
            query.add_filter("ticker", '=', ticker)
            query.add_filter("IS_SHARPE DIFFERENCE SLIPPAGE", '>', 0.0)
            query.keys_only()
            retrievedModels = list(query.fetch())
            sharedDict[ticker] = len(retrievedModels)
            print(ticker, len(retrievedModels))
            return
        except:
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))
            sharedDict[ticker] = None
            return 

def getValidCounts(db):
    allTickers = dataAck.getAllTickersPlain()
    mpEngine = mp.get_context('fork')
    with mpEngine.Manager() as manager:
        returnDict = manager.dict()
        
        runningP = []
        for ticker in allTickers:
            
            while len(runningP) > 16:
                runningP = dataAck.cycleP(runningP)
            
            p = mpEngine.Process(target=getValidModelsByTicker, args=(db, ticker, returnDict, ))
            p.start()
            runningP.append(p)


        while len(runningP) > 0:
                runningP = dataAck.cycleP(runningP)
                
        storedData = {}  
        for ticker in storedTickers:
            try:
                if returnDict[ticker] is not None:
                    storedData[ticker] = returnDict[ticker]
            except:
                continue
            
        return storedData

print(getValidCounts(params.treeModels))
