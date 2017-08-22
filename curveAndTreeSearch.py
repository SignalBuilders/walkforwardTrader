import dataAck
import portfolio
import CurvePredictor
import TreePredictor
import curveTreeDB
import params
allTickers = dataAck.getAllTickersPlain()
while True:
    import random
    tickerToTrade = allTickers[random.randint(0, len(allTickers)) - 1]
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
    while True:
        s = sManager.createSeries()
        while s.checkValidity(s.transformJoinedData(joinedData[:"2016-01-01"])) == False:
            s = sManager.createSeries()
            

        # try:
        for lookback in [5, 10, 22, 44]:
            for prediction in [5, 7, 10, 15]:
                for radius in [0.3, 0.5, 0.7, 1.0]:
                    for minConfidence in [0.01, 0.05, 0.1, 0.2]:
                        for minNeighbors in [1, 10, 30]:
                            if random.uniform(0,1) < 0.8: ##RANDOMLY SKIP
                                continue
                            cPre = CurvePredictor.CurvePredictor(s, tickerToTrade, lookback, prediction, radius, minConfidence, minNeighbors)
                            algoReturn, factorReturn, predictions, slippageAdjustedReturn, rawPredictions = cPre.runModelHistorical(joinedData, earlyStop=True)
                            if algoReturn is None:
                                toLog = factorReturn
                                toLog["modelDescription"] = str(cPre.describe())
                                dataAck.logModel("Model Curve Stopped Early", toLog)
                                print("Model Curve Stopped Early", toLog)
                                continue
                            metrics = dataAck.vizResults(slippageAdjustedReturn[:-252], algoReturn[:-252], factorReturn[:-252], False)
                            print("TRAIN:", metrics)
                            if metrics["BETA"] < 0.6\
                                 and metrics["SHARPE"] > 0.5 and metrics["ACTIVITY"] > 0.2:
                                ##STORE
                                testMetrics = dataAck.vizResults(slippageAdjustedReturn[-252:], algoReturn[-252:], factorReturn[-252:], False)
                                print("TEST:", testMetrics)
                                curveTreeDB.storeModel(params.curveModels, cPre, cPre.formUploadDictionary(), metrics, testMetrics)
                            else:
                                toLog = {"modelDescription":str(cPre.describe())}
                                for k in metrics:
                                    toLog[k] = metrics[k]
                                dataAck.logModel("Model Curve Skipped", toLog)
        # except:
        #     print("FAILED", s.describe())
        #     dataAck.logModel("Series Failed", {
        #         "seriesDescription":str(s.describe())
        #     })

        
        ##RUN TREE SEARCH
        buildingBlocks = curveTreeDB.getModels(params.curveModels, ticker=tickerToTrade) + curveTreeDB.getModels(params.treeModels, ticker=tickerToTrade)
        print(buildingBlocks)
        if len(buildingBlocks) > 5:

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










        runsSeen += 1

        if runsSeen > 10:
            ##START NEW TICKER
            dataAck.logModel("Search Update", {
                "message":"restarting search with different ticker",
                "currentTicker":tickerToTrade
            })
            break

        


        