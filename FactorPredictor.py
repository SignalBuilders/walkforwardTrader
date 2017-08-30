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

class FactorPredictor:
    def __init__(self, ticker, position):
        self.targetTicker = ticker
        ##POSITION IS any number to multiply factor return by...typically 0 or 1 with 0 being short and 1 being long
        self.position = position

    def describe(self):
        return (self.targetTicker, self.position)

    def getHash(self):
        return hashlib.sha224(str((self.targetTicker, self.position)).encode('utf-8')).hexdigest()

    def runModelToday(self, dataOfInterest):
        return self.position

    def formUploadDictionary(self):
        toUpload = {}
        toUpload["ticker"] = self.targetTicker
        toUpload["position"] = self.position
        return toUpload

    def runModelHistorical(self, dataOfInterest):
        dailyFactorReturn = dataAck.getDailyFactorReturn(self.targetTicker, dataOfInterest)
        predictions = pd.DataFrame(dailyFactorReturn.apply(lambda x:-1.0 if self.position == 0 else 1.0, axis=1), columns=["Predictions"]).dropna()
        transformedPreds = predictions.join(dailyFactorReturn).dropna()
        returnStream = pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"])

        return returnStream, dailyFactorReturn, predictions, returnStream, predictions