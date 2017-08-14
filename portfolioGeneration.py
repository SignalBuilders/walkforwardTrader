import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import numpy as np

def getUniqueModels(allModels):
    ##MUST PASS IN MODEL ENTITY
    ##GETS BEST MODEL FOR EACH INPUT SERIES
    sequences = {} 
    for modelEntity in allModels:
        model = modelEntity["model"]
        thisSeq = str(model.inputSeries.windowSize) + "," + \
                      str(model.inputSeries.series.describe())
        if thisSeq not in sequences:
            sequences[thisSeq] = []
        sequences[thisSeq].append({
            "model":model,
            "info":modelEntity
        })
    modelsToReturn = []
    for seq in sequences:
        bestModel = None
        for modelInfo in sequences[seq]:
            if bestModel is None:
                bestModel = modelInfo
            else:
                if bestModel["info"]["STABILITY"] < modelInfo["info"]["STABILITY"]:
                    bestModel = modelInfo
        modelsToReturn.append(bestModel["model"])
    return modelsToReturn

def generateAllReturns(allModels, joinedData):
    aggregateReturns = None
    aggregatePredictions = None
    for mod in allModels:
        print(mod.describe())
        algoReturn, factorReturn, predictions =  mod.makePredictions(joinedData)
        algoReturn.columns = [str(mod.describe())]
        predictions.columns = [str(mod.describe())]
        if aggregateReturns is None:
            aggregateReturns = algoReturn
            aggregatePredictions = predictions
        else:
            aggregateReturns = aggregateReturns.join(algoReturn)
            aggregatePredictions = aggregatePredictions.join(predictions)
    return aggregateReturns, aggregatePredictions

def visualizeModels(modelReturns):
    from string import ascii_letters
    import numpy as np
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt
    from IPython.display import display

    sns.set(style="white")


    # Compute the correlation matrix
    corr = modelReturns.corr()
    display(corr)

    # Generate a mask for the upper triangle
    mask = np.zeros_like(corr, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    from matplotlib.colors import ListedColormap

    # construct cmap
    flatui = ["#9b59b6", "#3498db", "#95a5a6", "#e74c3c", "#34495e", "#2ecc71"]
    cmap = ListedColormap(sns.color_palette(flatui).as_hex())
    
    # Draw the heatmap with the mask and correct aspect ratio
    sns.heatmap(corr, mask=mask, cmap=cmap, center=0,
                square=True, linewidths=.5, cbar_kws={"shrink": .5})
    plt.show()
    
    sns.set(style="white")
    
    # Compute the covariance matrix
    cov = modelReturns.cov()
    display(cov)
    # Generate a mask for the upper triangle
    mask = np.zeros_like(cov, dtype=np.bool)
    mask[np.triu_indices_from(mask)] = True

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(11, 9))

    # Generate a custom diverging colormap
    flatui = ["#9b59b6", "#3498db", "#95a5a6", "#e74c3c", "#34495e", "#2ecc71"]
    cmap = ListedColormap(sns.color_palette(flatui).as_hex())

    # Draw the heatmap with the mask and correct aspect ratio
    sns.heatmap(cov, mask=mask, cmap=cmap, center=0,
                square=True, linewidths=.5, cbar_kws={"shrink": .5})
    plt.show()



def storePastPredictions(allModels, modelPredictions):
    ##THESE ARE SUMMED PREDICTIONS...DIFFERENT THAN PREDICTIONS MADE DAILY
    lastDayUsedPredictions = modelPredictions.dropna()
    for i in range(len(lastDayUsedPredictions.columns)):
        thisModel = allModels[i]
        print(thisModel.describe())
        thisDF = lastDayUsedPredictions[[lastDayUsedPredictions.columns[i]]]
        predictionsToStore = []
        for j in range(len(thisDF.values)):
#             print(thisDF.index[i], thisDF.values[i][0])
            predictionsToStore.append(portfolio.storeAggregateModelPrediction(thisModel, thisDF.values[j][0], thisDF.index[j], shouldReturn=True))
        portfolio.storeManyItems(predictionsToStore)
            


def storePortfolio(models, description, benchmark):
    allHashes = []
    for model in models:
        organismHash = hashlib.sha224(str(model.describe()).encode('utf-8')).hexdigest()
        allHashes.append(organismHash)
    
    allHashes.sort()
    portfolioHash = hashlib.sha224(str(allHashes).encode('utf-8')).hexdigest()
    print("PORTFOLIO HASH:", portfolioHash)
    for hashing in allHashes:
        print(hashing)
        
    
        ##UPLOAD ORGANISM OBJECT
        while True:
            try:
                toUpload = {
                    "portfolio":portfolioHash,
                    "model":hashing
                }
                datastoreClient = datastore.Client('money-maker-1236')
                #HASH DIGEST
                key = datastoreClient.key(params.portfolioDB, hashlib.sha224(str(hashing + portfolioHash).encode('utf-8')).hexdigest()) #NEED TO HASH TO ENSURE UNDER COUNT
                organismToStore = datastore.Entity(key=key)
                organismToStore.update(toUpload)
                datastoreClient.put(organismToStore)
                break
            except:
                print("UPLOAD ERROR:", str(sys.exc_info()))
                time.sleep(10)
    
    ##STORE PORTFOLIO OBJECT
    while True:
        try:
            toUpload = {
                "description":description,
                "benchmark":benchmark
            }
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            key = datastoreClient.key(params.portfolioLookup, portfolioHash) #NEED TO HASH TO ENSURE UNDER COUNT
            organismToStore = datastore.Entity(key=key)
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)













