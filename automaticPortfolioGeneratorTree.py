
# coding: utf-8

# In[15]:

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

# In[16]:

## GET ALL MODELS


allModels = [item["model"] for item in curveTreeDB.getModels(ticker=None, returnEntireObject=True)]

for i in range(len(allModels)):
    model = allModels[i]
    print(model.describe())


# In[17]:

factorToTrade = "SPY"
uniqueModels, modelReturns, modelPredictions, modelSlippageReturns, modelReturnsWithFactor, joinedData = autoPortfolioTree.computeReturnsForUniqueModelsCache(allModels, factorToTrade)



# # PREPARE FOR AUTOMATIC GENERATOR -> EVERYTHING FROM THIS POINT ON SHOULD BE UNCHANGED

# In[19]:

aggregateReturns, aggregatePredictions = portfolioGeneration.generateAggregateReturnsPredictions(allModels, joinedData)
cleanedReturns = aggregateReturns.dropna()
cleanedPredictions = aggregatePredictions.dropna()
hashToModel = {}
for item in allModels:
    hashToModel[portfolio.getModelHash(item)] = item



# In[ ]:
print("STARTING GENERATION")
autoPortfolioTree.createPossiblePortfoliosMP(cleanedPredictions, cleanedReturns, hashToModel, joinedData, threadsToUse=16)


# In[ ]:



