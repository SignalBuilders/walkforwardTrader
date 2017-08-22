import dataAck
import portfolio
import CurvePredictor
import TreePredictor
import curveTreeDB
import params
allTickers = dataAck.getAllTickersPlain()
while True:
    import random
    tickerToTrade = "AGG" #allTickers[random.randint(0, len(allTickers)) - 1]
    print(tickerToTrade)
    
    tData = dataAck.getTrainingData(tickerToTrade)
    joinedData = None
    validTickers = None

    
    
    if tData is None:
        dataAck.logModel("Cache", {
            "type":"miss",
            "ticker":tickerToTrade,
            "day":str(portfolio.getToday())
        })

        tickersToPull = dataAck.getDataSourcesForTicker(tickerToTrade)
        print(tickersToPull)

        pulledData, validTickers = dataAck.downloadTickerData(tickersToPull)

        joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])
        
        dataAck.storeTrainingData(tickerToTrade, (joinedData, validTickers))
    else:
        joinedData = tData[0]
        validTickers = tData[1]
        dataAck.logModel("Cache", {
            "type":"hit",
            "ticker":tickerToTrade,
            "day":str(portfolio.getToday())
        })
        
    
    sManager = dataAck.seriesManager(validTickers)
    print(sManager.describe())

    import time
    import warnings
    import numpy as np
    warnings.filterwarnings("ignore")
    ##GET ALGOS INITIALLY GOOD
    runsSeen = 0

        
    ##RUN TREE SEARCH
    buildingBlocks = curveTreeDB.getModels(params.curveModels, ticker=tickerToTrade) + curveTreeDB.getModels(params.treeModels, ticker=tickerToTrade)
    print(buildingBlocks)
    if len(buildingBlocks) > 5:
        while True:
            try:
                blocksToUse = np.random.choice(buildingBlocks, 2, replace=False)
                tPre = TreePredictor.TreePredictor(blocksToUse[0], blocksToUse[1], "OR") 

                algoReturn, factorReturn, predictions, slippageAdjustedReturn, rawPredictions = tPre.runModelHistorical(joinedData)
                metrics = dataAck.vizResults(slippageAdjustedReturn[:-252], algoReturn[:-252], factorReturn[:-252], False)
                print("TRAIN:", metrics)
                if metrics["BETA"] < 0.6\
                     and metrics["SHARPE"] > 0.75 and metrics["ACTIVITY"] > 0.2:
                    ##STORE
                    testMetrics = dataAck.vizResults(slippageAdjustedReturn[-252:], algoReturn[-252:], factorReturn[-252:], False)
                    print("TEST:", testMetrics)
                    curveTreeDB.storeModelData(params.treeModelData, tPre, algoReturn, predictions, slippageAdjustedReturn)
                    curveTreeDB.storeModel(params.treeModels, tPre, tPre.formUploadDictionary(), metrics, testMetrics)
                else:
                    toLog = {"modelDescription":str(tPre.describe())}
                    for k in metrics:
                        toLog[k] = metrics[k]
                    dataAck.logModel("Model Tree Skipped", toLog)
            except:
                print("COMBO FAILED")

            runsSeen += 1

            if runsSeen > 10:
                ##START NEW TICKER
                dataAck.logModel("Search Update", {
                    "message":"restarting search with different ticker",
                    "currentTicker":tickerToTrade
                })
                break

        


        