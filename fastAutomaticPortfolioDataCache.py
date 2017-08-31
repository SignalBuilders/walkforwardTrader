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


# In[ ]:
print("STARTING OBJECT DOWNLOAD")

##DISABLE TO TEST FACTORS
treeModels = []
tickersSeen = []
dataObjs = curveTreeDB.getValidModels(params.treeModels, returnEntireObject=True)

for item in dataObjs:
    try:
        if item["IS_PROFITABILITY SLIPPAGE"] > 0.51 and item["IS_ANNUALIZED RETURN"] > 0.05: #and item["IS_BETA"] < 0.15:
            model = item["model"]
            print(model.targetTicker, item["IS_BETA"], item["OOS_BETA"], item["IS_ANNUALIZED RETURN"], item["OOS_ANNUALIZED RETURN"])
            treeModels.append(model)
            if model.targetTicker not in tickersSeen:
                tickersSeen.append(model.targetTicker)
    except:
        continue

print("MODELS ACCEPTED:", len(treeModels))
print("TICKERS ACCEPTED:", len(tickersSeen))


# In[ ]:

# len(allModels)


# In[ ]:

# len(tickersSeen)


# In[ ]:

import random
factorToTrade = "VTI"#tickersSeen[random.randint(0, len(tickersSeen) - 1)]
# factorToTrade


# In[ ]:

uniqueModels, modelReturns, modelPredictions, modelSlippageReturns, modelReturnsWithFactor, joinedData = autoPortfolioTree.computeReturnsForUniqueModelsCache(treeModels, factorToTrade)


# In[ ]:

cleanedReturns = modelReturns.fillna(0)
cleanedReturns.columns = [item.getHash() for item in uniqueModels]
cleanedReturns = cleanedReturns["2008-01-01":]

cleanedPredictions = modelPredictions.fillna(0)
cleanedPredictions.columns = [item.getHash() for item in uniqueModels]
cleanedPredictions = cleanedPredictions["2008-01-01":]
hashToModel = {}
for item in uniqueModels:
    hashToModel[item.getHash()] = item


# In[ ]:

# cleanedReturns


def storePortfolioInputData(cleanedReturns, cleanedPredictions, hashToModel, joinedData):
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(params.validModelsCache)
            blob = storage.Blob(params.validModelsLookup, bucket)
            blob.upload_from_string(pickle.dumps((cleanedReturns, cleanedPredictions, hashToModel, joinedData)))
            print("STORING", params.validModelsLookup)
            break
        except:
            print("UPLOAD BLOB ERROR:", str(sys.exc_info()))
            time.sleep(10)
    pass

##NEED TO STORE

storePortfolioInputData(cleanedReturns, cleanedPredictions, hashToModel, joinedData)