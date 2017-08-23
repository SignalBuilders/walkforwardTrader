import dataAck
import portfolio
import CurvePredictor
import TreePredictor
import curveTreeDB
import params
from google.cloud import error_reporting

client = error_reporting.Client('money-maker-1236', service="Tree Search", version=params.curveAndTreeVersion)
try:
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

            
        ##RUN TREE SEARCH
        curveBlocks = curveTreeDB.getModels(params.curveModels, ticker=tickerToTrade) 
        treeBlocks = curveTreeDB.getModels(params.treeModels, ticker=tickerToTrade)
        for buildingBlocks in [curveBlocks, treeBlocks]:
            runsSeen = 0
            attempts = 0
            print(buildingBlocks)
            if len(buildingBlocks) > 20:
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

                    attempts += 1

                    if runsSeen > 50 or attempts > 100:
                        # dataAck.logModel("Tree Search Stopped Early", {"runsSeen":runsSeen, "attempts":attempts})

                        break

        # RUN TREE SEARCH WITH COMBOS
        runsSeen = 0
        attempts = 0
        if len(curveBlocks) > 20 and len(treeBlocks) > 20:
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

                
                attempts += 1
                if runsSeen > 50 or attempts > 100:
                    # if runsSeen < 10:
                        # dataAck.logModel("Tree Search Stopped Early", {"runsSeen":runsSeen, "attempts":attempts})

                    break
except:
    client.report_exception()