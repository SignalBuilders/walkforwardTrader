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
# dataObjs = curveTreeDB.getValidModels(params.treeModels, returnEntireObject=True)

# for item in dataObjs:
#     try:
#         if item["IS_PROFITABILITY SLIPPAGE"] > 0.51 and item["IS_ANNUALIZED RETURN"] > 0.05:
#             model = item["model"]
#             print(model.targetTicker, model.getHash(), item["IS_SHARPE SLIPPAGE"], item["IS_SHARPE DIFFERENCE SLIPPAGE"], item["IS_BETA"])
#             allModels.append(model)
#             if model.targetTicker not in tickersSeen:
#                 tickersSeen.append(model.targetTicker)
#     except:
#         continue


##ADD FACTOR PREDICTOR
factorModels = []
factorObjs = curveTreeDB.getModels(params.factorModels, returnEntireObject=True)
for item in factorObjs:
    try:
        model = item["model"]
        print(model.targetTicker, model.getHash())
        factorModels.append(model)
        if model.targetTicker not in tickersSeen:
            tickersSeen.append(model.targetTicker)
    except:
        continue

# In[ ]:

# len(allModels)


# In[ ]:

# len(tickersSeen)


# In[ ]:

import random
factorToTrade = "VTI"#tickersSeen[random.randint(0, len(tickersSeen) - 1)]
# factorToTrade


# In[ ]:

uniqueModels, modelReturns, modelPredictions, modelSlippageReturns, modelReturnsWithFactor, joinedData = autoPortfolioTree.computeReturnsForUniqueModelsCache(treeModels, factorModels, factorToTrade)


# In[ ]:

cleanedReturns = modelReturns.fillna(0)
cleanedReturns.columns = [item.getHash() for item in uniqueModels]

cleanedPredictions = modelPredictions.fillna(0)
cleanedPredictions.columns = [item.getHash() for item in uniqueModels]
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