import dataAck
import portfolio
allTickers = dataAck.getAllTickersPlain()
while True:
    import random
    tickerToTrade = allTickers[random.randint(0, len(allTickers)) - 1]
    print(tickerToTrade)

    tickersToPull = dataAck.getDataSourcesForTicker(tickerToTrade)
    print(tickersToPull)

    pulledData, validTickers = dataAck.downloadTickerData(tickersToPull)

    joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])

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

        try:
            for defaultWindowSize in [10, 22, 44]:
                for trees in [25, 50, 100, 150]:
                    for predictionLength in [2, 3, 5]:
                        if random.uniform(0,1) < 0.7:
                            ##RANDOMLY SKIP
                            continue
                        b = dataAck.algoBlob(s, defaultWindowSize, trees, predictionLength, tickerToTrade)
                        algoReturn, factorReturn, predictions =  b.makePredictions(portfolio.prepareDataForModel(b, joinedData))
                        metrics = dataAck.vizResults(algoReturn[:-252], factorReturn[:-252], False)
                        print("TRAIN:", metrics)
                        if np.isnan(metrics["SHARPE"]) == True:
                            raise ValueError('SHARPE IS NAN SO FAULTY SERIES')
                        if metrics["SHARPE"] > 1.0 and metrics["ACTIVITY"] > 0.7 and metrics["BETA"] < 0.3:
                            ##STORE
                            testMetrics = dataAck.vizResults(algoReturn[-252:], factorReturn[-252:], False)
                            print("TEST:", testMetrics)
                            print("TODAY:", b.makeTodayPrediction(portfolio.prepareDataForModel(b, joinedData)))

                            dataAck.storeModel(b, metrics, testMetrics)
        except:
            print("FAILED", s.describe())
            dataAck.logModel("Search Update", {
                "message":"series failed",
                "seriesDescription":str(s.describe())
            })
            time.sleep(10)

        runsSeen += 1

        if runsSeen > 10:
            ##START NEW TICKER
            dataAck.logModel("Search Update", {
                "message":"restarting search with different ticker",
                "currentTicker":tickerToTrade
            })
            break

        