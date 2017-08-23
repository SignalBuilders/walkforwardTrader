
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
from google.cloud import error_reporting

client = error_reporting.Client('money-maker-1236', service="Automatic Portfolio Generator", version=params.curveAndTreeVersion)
try:

	allModels = [item["model"] for item in curveTreeDB.getModels(params.treeModels, ticker=None, returnEntireObject=True)]

	for i in range(len(allModels)):
	    model = allModels[i]
	    print(model.describe(), model.getHash())


	# In[17]:

	factorToTrade = "SPY"
	uniqueModels, modelReturns, modelPredictions, modelSlippageReturns, modelReturnsWithFactor, joinedData = autoPortfolioTree.computeReturnsForUniqueModelsCache(allModels, factorToTrade)



	# # PREPARE FOR AUTOMATIC GENERATOR -> EVERYTHING FROM THIS POINT ON SHOULD BE UNCHANGED

	# In[19]:

	aggregateReturns, aggregatePredictions = autoPortfolioTree.generateAggregateReturnsPredictions(allModels, joinedData)
	cleanedReturns = aggregateReturns.dropna()
	cleanedPredictions = aggregatePredictions.dropna()
	hashToModel = {}
	for item in allModels:
	    hashToModel[item.getHash()] = item



	# In[ ]:
	print("STARTING GENERATION")

	##REMOVE BREAK TO DO FULL AUTO
	autoPortfolioTree.createPossiblePortfoliosMP(cleanedPredictions, cleanedReturns, hashToModel, joinedData, threadsToUse=0)
except:
	client.report_exception()


# In[ ]:



