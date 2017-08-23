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


# In[16]:

## GET ALL MODELS

from google.cloud import error_reporting

client = error_reporting.Client('money-maker-1236', service="Result Cacher", version=params.curveAndTreeVersion)
try:
	allModels = [item["model"] for item in curveTreeDB.getModels(params.treeModels, ticker=None, returnEntireObject=True)]

	for i in range(len(allModels)):
	    model = allModels[i]
	    print(model.describe())


	# In[17]:

	factorToTrade = "SPY"
	uniqueModels, modelReturns, modelPredictions, modelSlippageReturns, modelReturnsWithFactor, joinedData = autoPortfolioTree.computeReturnsForUniqueModelsCache(allModels, factorToTrade)


	# # ENSURE ALL MODELS HAVE PREDICTIONS IN SYSTEM AND BACKFILLED
	# 

	# In[18]:

	for item in allModels:
	    modelStatus = autoPortfolioTree.checkAggregatePredictionsStored(item)
	    print(item.describe(), modelStatus)
	    if modelStatus == False:
	        ##NEED TO STORE MODEL PREDICTIONS
	        curveTreeDB.storePastPredictions([item], modelPredictions[[str(item.describe())]])
	        
	        ##NEED TO BACKFILL DAYS NOT CACHED
	        autoPortfolioTree.runBackfillMP(item, joinedData, 16)

except:
	client.report_exception()
