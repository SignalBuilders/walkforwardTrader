import dataAck
allTickers = dataAck.getAllTickersPlain()
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
while True:
    s = sManager.createSeries()
    while s.checkValidity(s.transformJoinedData(joinedData[:"2016-01-01"])) == False:
        s = sManager.createSeries()

    try:
        for defaultWindowSize in [10, 22]:
            for trees in [25, 50, 100]:
                for predictionLength in [2, 3, 5]:
                    b = dataAck.algoBlob(s, defaultWindowSize, trees, predictionLength, tickerToTrade)
                    algoReturn, factorReturn, predictions =  b.makePredictions(joinedData)
                    metrics = dataAck.vizResults(algoReturn[:-252], factorReturn[:-252], False)
                    print("TRAIN:", metrics)
                    if np.isnan(metrics["SHARPE"]) == True:
                        raise ValueError('SHARPE IS NAN SO FAULTY SERIES')
                    if metrics["SHARPE"] > -10.0:
                        ##STORE
                        testMetrics = dataAck.vizResults(algoReturn[-252:], factorReturn[-252:], False)
                        print("TEST:", testMetrics)
                        print("TODAY:", b.makeTodayPrediction(joinedData))

                        dataAck.storeModel(b, metrics, testMetrics)
    except:
        print("FAILED", s.describe())
        time.sleep(10)
        