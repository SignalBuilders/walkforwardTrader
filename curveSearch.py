import dataAck
import portfolio
import CurvePredictor
import TreePredictor
import curveTreeDB
import params
import datetime 
from google.cloud import error_reporting

client = error_reporting.Client('money-maker-1236', service="Curve Search", version=params.curveAndTreeVersion)

try:
    allTickers = dataAck.getAllTickersPlain()

    ##SHOULD BE PRE-INITIALIZED
    tData = dataAck.getTrainingData(params.tickerDataLookup)
    joinedData = tData[0]
    validTickers = tData[1]
    while True:
        startTime = datetime.datetime.now()
        import random
        ##ADVANCED TICKER TO TRADE SELECTION
        modelCount, modelSplitByTicker, predictionCount, numPredictors = curveTreeDB.getModelCounts(params.curveModels)

        validTickersToTrade = []

        for ticker in allTickers:
            if ticker not in modelSplitByTicker:
                validTickersToTrade.append(ticker)
                print("NOT PRESENT", ticker)

        if len(validTickersToTrade) == 0:
            ##MEANS ALL TICKERS HAVE AT LEAST ONE MODEL
            for ticker in sorted(modelSplitByTicker, key=modelSplitByTicker.get)[:40]:
                validTickersToTrade.append(ticker)
                print(ticker, modelSplitByTicker[ticker])



        tickerToTrade = validTickersToTrade[random.randint(0, len(validTickersToTrade)) - 1]
        print(tickerToTrade)

        
        
        

        
        
        # if tData is None:
        #     dataAck.logModel("Cache", {
        #         "type":"miss",
        #         "ticker":tickerToTrade,
        #         "day":str(portfolio.getToday())
        #     })

        #     tickersToPull = dataAck.getDataSourcesForTicker(tickerToTrade)
        #     print(tickersToPull)

        #     pulledData, validTickers = dataAck.downloadTickerData(tickersToPull)

        #     joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])
            
        #     dataAck.storeTrainingData(tickerToTrade, (joinedData, validTickers))
        # else:
        
            # dataAck.logModel("Cache", {
            #     "type":"hit",
            #     "ticker":tickerToTrade,
            #     "day":str(portfolio.getToday())
            # })
            
        print("DATASOURCES:", len(validTickers))
        
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
                                        # dataAck.logModel("Model Curve Stopped Early", toLog)
                                        print("Model Curve Stopped Early", toLog)
                                        continue
                                    metrics = dataAck.vizResults(slippageAdjustedReturn[:-252], algoReturn[:-252], factorReturn[:-252], False)
                                    print("TRAIN:", metrics)
                                    if metrics["RAW BETA"] < 0.6 and metrics["TOTAL DAYS SEEN"] >= 1700\
                                         and (metrics["SHARPE"] > 0.5 or metrics["SHARPE DIFFERENCE"] > 0.0) and metrics["ACTIVITY"] > 0.2 and metrics["STABILITY"] > 0.5:
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

            if runsSeen > 40:
                ##START NEW TICKER
                dataAck.logModel("Search Update", {
                    "message":"restarting search with different ticker",
                    "currentTicker":tickerToTrade,
                    "elapsedTime":str(datetime.datetime.now() - startTime)
                })
                break

        ##BASIC TREE SEARCH
        # startTime = datetime.datetime.now()
        # curveBlocks = curveTreeDB.getModels(params.curveModels, ticker=tickerToTrade) 
        # buildingBlocks = curveBlocks
        # runsSeen = 0
        # attempts = 0
        # print(buildingBlocks)
        # if len(buildingBlocks) > 30:
        #     while True:
        #         try:
        #             blocksToUse = np.random.choice(buildingBlocks, 2, replace=False)
        #             tPre = TreePredictor.TreePredictor(blocksToUse[0], blocksToUse[1], "OR" if random.uniform(0,1) < 0.5 else "AND") 
        #             if curveTreeDB.modelExists(params.treeModels, tPre.getHash()) == True:
        #                 dataAck.logModel("Model Tree Already Exists", {"numPredictors":tPre.numberOfPredictors()})
        #                 raise ValueError("Tree Model Already Exists") 
        #             curveTreeDB.logModelAttempted(tPre)
        #             algoReturn, factorReturn, predictions, slippageAdjustedReturn, rawPredictions = tPre.runModelHistorical(joinedData)
        #             metrics = dataAck.vizResults(slippageAdjustedReturn[:-252], algoReturn[:-252], factorReturn[:-252], False)
        #             print("TRAIN:", metrics)
        #             if (metrics["SHARPE"] > 0.75 or metrics["SHARPE DIFFERENCE"] > 0.0)\
        #                  and metrics["ACTIVITY"] > 0.4 and metrics["RAW BETA"] < 0.6\
        #                  and metrics["STABILITY"] > 0.6 and metrics["TOTAL DAYS SEEN"] >= 1700:
        #                 ##STORE
        #                 testMetrics = dataAck.vizResults(slippageAdjustedReturn[-252:], algoReturn[-252:], factorReturn[-252:], False)
        #                 print("TEST:", testMetrics)
        #                 curveTreeDB.storeModelData(params.treeModelData, tPre, algoReturn, predictions, slippageAdjustedReturn)
        #                 curveTreeDB.storeModel(params.treeModels, tPre, tPre.formUploadDictionary(), metrics, testMetrics)
        #             else:
        #                 toLog = {"modelDescription":str(tPre.describe())}
        #                 for k in metrics:
        #                     toLog[k] = metrics[k]
        #                 dataAck.logModel("Model Tree Skipped", toLog)
        #             runsSeen += 1
        #         except Exception as e:
        #             print("COMBO FAILED", e)

        #         attempts += 1

        #         if runsSeen > 10 or attempts > 30:
        #             dataAck.logModel("Search Update", {
        #                 "message":"finished tree search in curve",
        #                 "runsSeen":runsSeen, 
        #                 "attempts":attempts,
        #                 "currentTicker":tickerToTrade,
        #                 "elapsedTime":str(datetime.datetime.now() - startTime)
        #                 })

        #             break

except:
    client.report_exception()





