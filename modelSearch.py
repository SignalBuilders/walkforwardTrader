import dataAck
import portfolio
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
        for defaultWindowSize in [10, 22, 44]:
            for trees in [25, 50, 100, 150, 300]:
                for predictionLength in [5, 7, 10, 15]:
                    if random.uniform(0,1) < 0.7: ##RANDOMLY SKIP
                        continue
                    b = dataAck.algoBlob(s, defaultWindowSize, trees, predictionLength, tickerToTrade)
                    algoReturn, factorReturn, predictions, slippageAdjustedReturn =  b.makePredictions(portfolio.prepareDataForModel(b, joinedData), daysToCheck = None, earlyStop = True)
                    if algoReturn is None:
                        toLog = factorReturn
                        if np.isnan(toLog["sharpe"]) == True:
                            raise ValueError('SHARPE IS NAN SO FAULTY SERIES')
                            
                        toLog["modelDescription"] = str(b.describe())
                        dataAck.logModel("Model Stopped Early", toLog)
                        print("Model Stopped Early", toLog)
                        continue
                    metrics = dataAck.vizResults(slippageAdjustedReturn[:-252], algoReturn[:-252], factorReturn[:-252], False)
                    print("TRAIN:", metrics)
                    if np.isnan(metrics["SHARPE"]) == True:
                        raise ValueError('SHARPE IS NAN SO FAULTY SERIES')
                    if metrics["ACTIVITY"] > 0.7 and metrics["BETA"] < 0.6\
                         and metrics["SHARPE DIFFERENCE SLIPPAGE"] > 0.0 and metrics["ALPHA SLIPPAGE"] > 0.03:
                        ##STORE
                        testMetrics = dataAck.vizResults(slippageAdjustedReturn[-252:], algoReturn[-252:], factorReturn[-252:], False)
                        print("TEST:", testMetrics)
                        print("TODAY:", b.makeTodayPrediction(portfolio.prepareDataForModel(b, joinedData)))
                        dataAck.storeModelData(b, algoReturn, predictions, slippageAdjustedReturn)
                        dataAck.storeModel(b, metrics, testMetrics)
                    else:
                        toLog = {"modelDescription":str(b.describe())}
                        for k in metrics:
                            toLog[k] = metrics[k]
                        dataAck.logModel("Model Skipped", toLog)
        # except:
            # print("FAILED", s.describe())
            # dataAck.logModel("Series Failed", {
            #     "seriesDescription":str(s.describe())
            # })

        runsSeen += 1

        if runsSeen > 10:
            ##START NEW TICKER
            dataAck.logModel("Search Update", {
                "message":"restarting search with different ticker",
                "currentTicker":tickerToTrade
            })
            break

        