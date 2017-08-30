import dataAck
import portfolio
import CurvePredictor
import TreePredictor
import FactorPredictor
import curveTreeDB
import params
import datetime 
from google.cloud import error_reporting


allTickers = dataAck.getAllTickersPlain()

##SHOULD BE PRE-INITIALIZED
tData = dataAck.getTrainingData(params.tickerDataLookup)
joinedData = tData[0]
validTickers = tData[1]
for tickerToTrade in validTickers:
    print(tickerToTrade)
    for pos in [0.0, 1.0]:
        fPre = FactorPredictor.FactorPredictor(tickerToTrade, pos)
        algoReturn, factorReturn, predictions, slippageAdjustedReturn, rawPredictions = fPre.runModelHistorical(joinedData)
        metrics = dataAck.vizResults(slippageAdjustedReturn[:-252], algoReturn[:-252], factorReturn[:-252], False)
        print(metrics)
        testMetrics = dataAck.vizResults(slippageAdjustedReturn[-252:], algoReturn[-252:], factorReturn[-252:], False)
        print("TEST:", testMetrics)
        curveTreeDB.storeModelData(params.factorModelData, fPre, algoReturn, predictions, slippageAdjustedReturn)
        curveTreeDB.storeModel(params.factorModels, fPre, fPre.formUploadDictionary(), metrics, testMetrics)

        