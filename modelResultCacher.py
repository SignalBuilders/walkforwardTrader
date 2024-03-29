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
import autoPortfolio


# In[16]:

## GET ALL MODELS


allModels = [item["model"] for item in portfolio.getModels(ticker=None, returnEntireObject=True)]

for i in range(len(allModels)):
    model = allModels[i]
    print(model.describe())


# In[17]:

factorToTrade = "SPY"
uniqueModels, modelReturns, modelPredictions, modelSlippageReturns, modelReturnsWithFactor, joinedData =    autoPortfolio.computeReturnsForUniqueModelsCache(allModels, factorToTrade)


# # ENSURE ALL MODELS HAVE PREDICTIONS IN SYSTEM AND BACKFILLED
# 

# In[18]:

for item in allModels:
    modelStatus = autoPortfolio.checkAggregatePredictionsStored(item)
    print(item.describe(), modelStatus)
    if modelStatus == False:
        ##NEED TO STORE MODEL PREDICTIONS
        portfolioGeneration.storePastPredictions([item], modelPredictions[[str(item.describe())]])
        
        ##NEED TO BACKFILL DAYS NOT CACHED
        autoPortfolio.runBackfillMP(item, joinedData, 16)
