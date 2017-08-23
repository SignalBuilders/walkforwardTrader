import dataAck
import portfolio
import CurvePredictor
import TreePredictor
import curveTreeDB
import params
allTickers = dataAck.getAllTickersPlain()
while True:
    import random
    tickerToTrade = "AGG"#allTickers[random.randint(0, len(allTickers)) - 1]
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
        
        print("*********")
        print("NEW SERIES", s.describe())
        print("*********")

        try:
            for lookback in [5, 10, 22, 44]:
                for prediction in [2, 3, 5, 7, 10, 15]:
                    for radius in [0.3, 0.5, 0.7, 1.0, 1.5, 2.0]:
                        for minConfidence in [0.01, 0.05, 0.1, 0.2]:
                            for minNeighbors in [10]:
                                if random.uniform(0,1) < 0.97: ##RANDOMLY SKIP A LOT...FAILING FAST ON SERIES ALLOWS US TO EXAMINE MUCH LARGER SAMPLE SPACE
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
                                if metrics["RAW BETA"] < 0.6\
                                     and (metrics["SHARPE"] > 0.35 or metrics["SHARPE DIFFERENCE"] > 0.0) and metrics["ACTIVITY"] > 0.2:
                                    ##STORE
                                    testMetrics = dataAck.vizResults(slippageAdjustedReturn[-252:], algoReturn[-252:], factorReturn[-252:], False)
                                    print("TEST:", testMetrics)
                                    curveTreeDB.storeModel(params.curveModels, cPre, cPre.formUploadDictionary(), metrics, testMetrics)
                                else:
                                    toLog = {"modelDescription":str(cPre.describe())}
                                    for k in metrics:
                                        toLog[k] = metrics[k]
                                    dataAck.logModel("Model Curve Skipped", toLog)
        except:
            print("FAILED", s.describe())
            dataAck.logModel("Series Failed", {
                "seriesDescription":str(s.describe())
            })



        runsSeen += 1

        if runsSeen > 20:
            ##START NEW TICKER
            dataAck.logModel("Search Update", {
                "message":"restarting search with different ticker",
                "currentTicker":tickerToTrade
            })
            break

    ##RUN TREE SEARCH
    
    curveBlocks = curveTreeDB.getModels(params.curveModels, ticker=tickerToTrade) 
    treeBlocks = curveTreeDB.getModels(params.treeModels, ticker=tickerToTrade)
    for buildingBlocks in [curveBlocks, treeBlocks]:
        runsSeen = 0
        print(buildingBlocks)
        if len(buildingBlocks) > 5:
            while True:
                try:
                    blocksToUse = np.random.choice(buildingBlocks, 2, replace=False)
                    tPre = TreePredictor.TreePredictor(blocksToUse[0], blocksToUse[1], "OR" if random.uniform(0,1) < 0.5 else "AND") 

                    algoReturn, factorReturn, predictions, slippageAdjustedReturn, rawPredictions = tPre.runModelHistorical(joinedData)
                    metrics = dataAck.vizResults(slippageAdjustedReturn[:-252], algoReturn[:-252], factorReturn[:-252], False)
                    print("TRAIN:", metrics)
                    if (metrics["SHARPE"] > 0.5 or metrics["SHARPE DIFFERENCE"] > 0.0) and metrics["ACTIVITY"] > 0.2 and metrics["RAW BETA"] < 0.6:
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
                except:
                    print("COMBO FAILED")

                

                if runsSeen > 10:
                    ##START NEW TICKER
                    dataAck.logModel("Search Update", {
                        "message":"restarting search with different ticker",
                        "currentTicker":tickerToTrade
                    })
                    break
    # RUN TREE SEARCH WITH COMBOS
    runsSeen = 0
    if len(curveBlocks) > 5 and len(treeBlocks) > 5:
        while True:
            try:
                blocksToUse = [np.random.choice(curveBlocks, 1, replace=False)[0], np.random.choice(treeBlocks, 1, replace=False)[0]]
                tPre = TreePredictor.TreePredictor(blocksToUse[0], blocksToUse[1], "OR" if random.uniform(0,1) < 0.5 else "AND") 

                algoReturn, factorReturn, predictions, slippageAdjustedReturn, rawPredictions = tPre.runModelHistorical(joinedData)
                metrics = dataAck.vizResults(slippageAdjustedReturn[:-252], algoReturn[:-252], factorReturn[:-252], False)
                print("TRAIN:", metrics)
                if (metrics["SHARPE"] > 0.5 or metrics["SHARPE DIFFERENCE"] > 0.0) and metrics["ACTIVITY"] > 0.2 and metrics["RAW BETA"] < 0.6:
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
            except:
                print("COMBO FAILED")

            

            if runsSeen > 10:
                ##START NEW TICKER
                dataAck.logModel("Search Update", {
                    "message":"restarting search with different ticker",
                    "currentTicker":tickerToTrade
                })
                break