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




class AverageTreePredictor:
    def __init__(self, obj1, obj2):
        self.obj1 = obj1
        self.obj2 = obj2

        ##CHECK SIMPLE TESTS
        
        if self.obj1.targetTicker != self.obj2.targetTicker:
            raise ValueError("TARGET TICKER NOT WELL DEFINED") 

        ##CHECK NO OVERLAP
        obj1Hashes = self.obj1.getAllHashes()
        obj2Hashes = self.obj2.getAllHashes()
        for possibleHash in obj1Hashes:
            if possibleHash in obj2Hashes:
                raise ValueError("OVERLAP OF MODELS")
        
        if self.obj1.predictionDistance != self.obj2.predictionDistance:
            print("WARNING", "PREDICTION DISTANCE NOT WELL DEFINED", self.obj1.predictionDistance, self.obj2.predictionDistance) 
        
        self.predictionDistance = max(self.obj1.predictionDistance, self.obj2.predictionDistance) ##SHOULD BE SAME FOR BOTH OBJECTS
        self.targetTicker = self.obj1.targetTicker

    def describe(self):
        return (self.obj1.describe(), self.obj2.describe(), self.predictionDistance)

    def getHash(self):
        hash1 = hashlib.sha224(str((self.obj1.describe(), self.obj2.describe(), self.predictionDistance)).encode('utf-8')).hexdigest()
        hash2 = hashlib.sha224(str((self.obj2.describe(), self.obj1.describe(), self.predictionDistance)).encode('utf-8')).hexdigest()
        return hashlib.sha224((hash1 + hash2).encode('utf-8')).hexdigest() if hash1 < hash2 else hashlib.sha224((hash2 + hash1).encode('utf-8')).hexdigest()

    def getAllHashes(self):
        ##RETURN ALL HASHES INCLUDING REVERSED
        return self.obj1.getAllHashes() + self.obj2.getAllHashes() + [self.getHash()] 

    def formUploadDictionary(self):
        toUpload = {}
        toUpload["ticker"] = self.targetTicker
        toUpload["predictionLength"] = self.predictionDistance
        toUpload["numberOfPredictors"] = self.numberOfPredictors()
        return toUpload

    def numberOfPredictors(self):
        return self.obj1.numberOfPredictors() + self.obj2.numberOfPredictors()

    def returnAllTickersInvolved(self):
        return self.obj1.returnAllTickersInvolved() + self.obj2.returnAllTickersInvolved()

    def runModelToday(self, dataOfInterest):
        return self.combinePredictions([dataAck.computePositionConfidence([self.obj1.runModelToday(dataOfInterest)]), dataAck.computePositionConfidence([self.obj2.runModelToday(dataOfInterest)])])

    def combinePredictions(self, predictionArr):
        return (predictionArr[0] + predictionArr[1])/2.0


    def runModelHistorical(self, dataOfInterest):

        ##RAW PREDICTIONS ARE PREDS 0->1.0
        returnStream, factorReturn, predictions, slippageAdjustedReturn, rawPredictions1 = self.obj1.runModelHistorical(dataOfInterest)
        returnStream, factorReturn, predictions, slippageAdjustedReturn, rawPredictions2 = self.obj2.runModelHistorical(dataOfInterest)

        #computePositionConfidence 
        print(rawPredictions1)
        print(rawPredictions2)
        positions1 = pd.DataFrame(rawPredictions1.apply(lambda x:dataAck.computePositionConfidence(x), axis=1), columns=["Positions 1"]).dropna()
        positions2 = pd.DataFrame(rawPredictions2.apply(lambda x:dataAck.computePositionConfidence(x), axis=1), columns=["Positions 2"]).dropna()


        positions = positions1.join(positions2).dropna()

        # print(rawPredictions)
        #averagePredictions
        positionsTable = pd.DataFrame(positions.apply(lambda x:self.combinePredictions(x), axis=1, raw=True))
        rawPositions = positionsTable
        ##PREDICTIONS COMBINED AS 0, 0.5, 1 where

        i = 1
        tablesToJoin = []
        while i < self.predictionDistance:
            thisTable = positionsTable.shift(i)
            thisTable.columns = ["Positions_" + str(i)]
            tablesToJoin.append(thisTable)
            i += 1

        returnStream = None
        factorReturn = None
        predictions = None
        slippageAdjustedReturn = None

        positionsTable = positionsTable.join(tablesToJoin)
        ##AVERAGE...A LOT OF SUBTLETY IN STRENGTH OF PREDICTION
        transformedPositions = pd.DataFrame(positionsTable.apply(lambda x:sum(x)/len(x), axis=1), columns=["Positions"]).dropna()
        dailyFactorReturn = dataAck.getDailyFactorReturn(self.targetTicker, dataOfInterest)
        transformedPositions = transformedPositions.join(dailyFactorReturn).dropna()
        returnStream = pd.DataFrame(transformedPositions.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"])
        factorReturn = pd.DataFrame(transformedPositions[["Factor Return"]])
        positions = pd.DataFrame(transformedPositions[["Positions"]])
        print("POSITIONS")
        print(positions)
        estimatedSlippageLoss = portfolioGeneration.estimateTransactionCost(positions)
        estimatedSlippageLoss.columns = returnStream.columns
        slippageAdjustedReturn = (returnStream - estimatedSlippageLoss).dropna()

        return returnStream, factorReturn, positions, slippageAdjustedReturn, rawPositions

