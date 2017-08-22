## MAKES A TREE PREDICTOR OUT OF MANY CURVE PREDICTORS
import pandas as pd
import multiprocessing as mp
import empyrical
import portfolioGeneration
import numpy as np
import dataAck
import portfolio

import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys




class TreePredictor:
    def __init__(self, obj1, obj2, combiner):
        self.obj1 = obj1
        self.obj2 = obj2
        if str(self.obj1.describe()) == str(self.obj2.describe()):
            raise ValueError("INPUT MODELS IDENTICAL") ##RAISE ERROR IN FUTURE
        if self.obj1.predictionDistance != self.obj2.predictionDistance:
            raise ValueError("PREDICTION DISTANCE NOT WELL DEFINED") ##RAISE ERROR IN FUTURE
        if self.obj1.targetTicker != self.obj2.targetTicker:
            raise ValueError("TARGET TICKER NOT WELL DEFINED") ##RAISE ERROR IN FUTURE
        self.predictionDistance = self.obj1.predictionDistance ##SHOULD BE SAME FOR BOTH OBJECTS
        self.targetTicker = self.obj1.targetTicker
        self.combiner = combiner ##AND or OR -> TREAT 0 AS SAME TYPE

    def describe(self):
        return (self.obj1.describe(), self.obj2.describe(), self.predictionDistance, self.combiner)

    def getHash(self):
        return hashlib.sha224(str(self.describe()).encode('utf-8')).hexdigest()

    def formUploadDictionary(self):
        toUpload = {}
        toUpload["ticker"] = self.targetTicker
        toUpload["predictionLength"] = self.predictionDistance
        toUpload["combiner"] = self.combiner
        toUpload["numberOfPredictors"] = self.numberOfPredictors()
        return toUpload

    def numberOfPredictors(self):
        return self.obj1.numberOfPredictors() + self.obj2.numberOfPredictors()

    def runModelToday(self, dataOfInterest):
        return self.combinePredictions([self.obj1.runModelToday(dataOfInterest), self.obj2.runModelToday(dataOfInterest)])

    def combinePredictions(self, predictionArr):
        if self.combiner == "AND":
            if predictionArr[0] == predictionArr[1]:
                if predictionArr[0] == 1:
                    return 1.0 ##BUY
                elif predictionArr[0] == 0:
                    return 0.5 ##NO POS
                else:
                    return 0.0 ##SHORT
            else:
                return 0.5
        elif self.combiner == "OR":
            if predictionArr[0] == predictionArr[1]:
                if predictionArr[0] == 1:
                    return 1.0 ##BUY
                elif predictionArr[0] == 0:
                    return 0.5 ##NO POS
                else:
                    return 0.0 ##SHORT
            elif predictionArr[0] == 0 and predictionArr[1] != 0:
                if predictionArr[1] == 1:
                    return 1.0 ##BUY
                elif predictionArr[1] == 0:
                    return 0.5 ##NO POS
                else:
                    return 0.0 ##SHORT
            elif predictionArr[0] != 0 and predictionArr[1] == 0:
                if predictionArr[0] == 1:
                    return 1.0 ##BUY
                elif predictionArr[0] == 0:
                    return 0.5 ##NO POS
                else:
                    return 0.0 ##SHORT
            elif predictionArr[0] != predictionArr[1]:
                return 0.5 ## NO POSITION


    def runModelHistorical(self, dataOfInterest):

        ##RAW PREDICTIONS ARE PREDS 0->1.0
        returnStream, factorReturn, predictions, slippageAdjustedReturn, rawPredictions1 = self.obj1.runModelHistorical(dataOfInterest)
        returnStream, factorReturn, predictions, slippageAdjustedReturn, rawPredictions2 = self.obj2.runModelHistorical(dataOfInterest)

        rawPredictions1 = pd.DataFrame(rawPredictions1.apply(lambda x:dataAck.computePosition(x), axis=1), columns=["Predictions 1"]).dropna()
        rawPredictions2 = pd.DataFrame(rawPredictions2.apply(lambda x:dataAck.computePosition(x), axis=1), columns=["Predictions 2"]).dropna()

        rawPredictions = rawPredictions1.join(rawPredictions2).dropna()

        predsTable = pd.DataFrame(rawPredictions.apply(lambda x:self.combinePredictions(x), axis=1, raw=True))
        rawPredictions = predsTable
        ##PREDICTIONS COMBINED AS 0, 0.5, 1 where

        i = 1
        tablesToJoin = []
        while i < self.predictionDistance:
            thisTable = predsTable.shift(i)
            thisTable.columns = ["Predictions_" + str(i)]
            tablesToJoin.append(thisTable)
            i += 1

        returnStream = None
        factorReturn = None
        predictions = None
        slippageAdjustedReturn = None

        predsTable = predsTable.join(tablesToJoin)
        transformedPreds = pd.DataFrame(predsTable.apply(lambda x:dataAck.computePosition(x), axis=1), columns=["Predictions"]).dropna()
        dailyFactorReturn = dataAck.getDailyFactorReturn(self.targetTicker, dataOfInterest)
        transformedPreds = transformedPreds.join(dailyFactorReturn).dropna()
        returnStream = pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"]) if returnStream is None else pd.concat([returnStream, pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"])])
        factorReturn = pd.DataFrame(transformedPreds[["Factor Return"]]) if factorReturn is None else pd.concat([factorReturn, pd.DataFrame(transformedPreds[["Factor Return"]])])
        predictions = pd.DataFrame(transformedPreds[["Predictions"]]) if predictions is None else pd.concat([predictions, pd.DataFrame(transformedPreds[["Predictions"]])])
        estimatedSlippageLoss = portfolioGeneration.estimateTransactionCost(predictions)
        estimatedSlippageLoss.columns = returnStream.columns
        slippageAdjustedReturn = (returnStream - estimatedSlippageLoss).dropna()

        return returnStream, factorReturn, predictions, slippageAdjustedReturn, rawPredictions









