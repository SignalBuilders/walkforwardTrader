##USE QUANDL AND PANDAS
import quandl
import pandas as pd
import portfolioGeneration

def getTickerData(ticker):
    """
    calls quandl API to download ticker data from /EOD

    :param ticker: ticker to fetch

    :returns: a dataframe of the ticker containing the adj_close price

    """
    limitedDf = quandl.get(["EOD/" + ticker +".11"], authtoken="G3AvFe4ZUZoBEthhjmEY")
    limitedDf.columns = ["Adj_Close_" + ticker]
    return limitedDf


import time
import multiprocessing as mp
import sys
def cycleP(runningProcesses):
    """
    cycles through running processes to free ones that have ended

    :param runningProcesses: list of previously running processes

    :returns: a list of currently running processes

    """
    newP = []
    for p in runningProcesses:
        if p.is_alive() == True:
            newP.append(p)
        else:
            p.join()
    return newP

def downloadTickerData(storedTickers):
    """
    downloads all related tickers using :meth:`dataAck.getTickerData`

    :param storedTickers: list of tickers to retrieve

    :returns: dictionary containing dataframes of valid tickers

    """
    mpEngine = mp.get_context('fork')
    with mpEngine.Manager() as manager:
        returnDict = manager.dict()
        ##CHILDREN TO SPAWN
        def plainSources(tick, sharedDict):
            try:
                thisData = getTickerData(tick)

                sharedDict[tick] = thisData
                print(tick)
            except:
                sharedDict[tick] = None
                print("FAILED:", tick)
                print("APPLICABLE ETF ERROR:", str(sys.exc_info()))
        
        runningP = []
        for ticker in storedTickers:
            
            while len(runningP) > 8:
                runningP = cycleP(runningP)
            
            p = mpEngine.Process(target=plainSources, args=(ticker, returnDict, ))
            p.start()
            runningP.append(p)


        while len(runningP) > 0:
                runningP = cycleP(runningP)
                
        storedData = {}  
        for ticker in storedTickers:
            try:
                if returnDict[ticker] is not None:
                    storedData[ticker] = returnDict[ticker]
            except:
                continue
            
        return storedData, list(storedData.keys())

def joinDatasets(tickerDatas):
    """
    forms one large dataframe from tickers retrieved using :meth:`dataAck.downloadTickerData`

    :param tickerDatas: list of tickers dataframes to join

    :returns: one large dataframe of ticker data

    .. note:: one large dataframe makes transformations easier, as they can be performed all at once

    """
    baseTicker = tickerDatas[0]
    for ticker in tickerDatas[1:]:
        baseTicker = baseTicker.join(ticker, how='outer')
    return baseTicker



import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import numpy as np

def getAllTickersPlain():
    """
    gets all tickers available to use for trading

    :returns: list of tickers to trade

    .. note:: this is based on average daily trading volume

    """
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.dataSourcesDatastoreName, projection=["ticker"], distinct_on=["ticker"])
            retrievedDatasources = list(query.fetch())
            toReturn = {}
            for source in retrievedDatasources:
                toReturn[source["ticker"]] = source["ticker"]

            return [item for item in toReturn]
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def getDataSourcesForTicker(ticker):
    """
    gets all tickers to use as sources for a given ticker

    :param ticker: ticker to use in query

    :returns: all applicable tickers

    .. note:: this is based on looking at tradeable tickers that have low beta to the given ticker

    """
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=params.dataSourcesDatastoreName)
            query.add_filter('ticker', '=', ticker)
            retrievedDatasources = list(query.fetch())
            toReturn = []
            for source in retrievedDatasources:
                toReturn.append(source["applicable_ticker"])

            return toReturn + [ticker]
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))

def getModelInformationByKey(modelHashes):
    """
    returns model infromation stored after model passes screening metrics

    :param modelHashes: all hashes sought to examine

    :returns: entities of modelHashes

    """
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            ##form keys
            keys = []
            for hashing in modelHashes:
                key = datastore_client.key(params.datastoreName, hashing)
                keys.append(key)
                
            retrievedModels = datastore_client.get_multi(keys)
            toReturn = []
            for source in retrievedModels:
                del source["model"]
                toReturn.append(source)
            return toReturn
            
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))


##SERIES MANIPULATION

def createPriceSeries(joinedData, ticker):
    """
    returns the price column of a large dataframe for a particular ticker
    this is not much more than a simple templating function

    :param joinedData: large dataframe of ticker data
    :param ticker: ticker to consider

    :returns: single column of large dataframe

    """
    return joinedData["Adj_Close_" + ticker]


def diffSeries(priceVector, diffAmount):
    """
    takes a pricevector series and finds the difference over some period

    :param priceVector: some vector of prices (could be modified already or not)
    :param diffAmount: amount by which to cause manipulation

    :returns: modified data

    """
    return (priceVector.diff(diffAmount)/priceVector).dropna()

def volSeries(priceVector, lookback):
    """
    takes a pricevector series and finds the std over some period

    :param priceVector: some vector of prices (could be modified already or not)
    :param lookback: amount by which to cause manipulation

    :returns: modified data

    """
    return (priceVector.rolling(window=lookback, min_periods=lookback).std()).dropna()

def rollingAvgSeries(priceVector, lookback):
    """
    takes a pricevector series and smooths the values over some window

    :param priceVector: some vector of prices (could be modified already or not)
    :param lookback: amount by which to cause manipulation

    :returns: modified data

    """
    return (priceVector.rolling(lookback, min_periods=lookback).mean()).dropna()

import random
class dataSeries:
    def __init__(self, ticker):
        """
        object created by :class:`dataAck.seriesManager` to store transformation instructions for raw price data

        :param ticker: ticker to use for all data manipulation      
        """
        self.ticker = ticker
        
        self.diffDays = None
        self.secondDiffDays = None
        self.volDays = None

        ##USE DIFF
        self.diffDays = random.randint(1, 44)

        if random.uniform(0,1) < 0.5:
            self.secondDiffDays = random.randint(1, 22)
            
        if random.uniform(0,1) < 0.3:
            self.volDays = random.randint(5, 22)
           
        self.smoothingDays = random.randint(1, 5)
        
        
    def transformJoinedData(self, joinedData):
        """
        takes a large dataframe and performs stored manipulations to it

        :param joinedData: large dataframe of raw price data

        :returns: modified data

        """
        underlyingSeries = createPriceSeries(joinedData, self.ticker)

        if self.diffDays is not None:
            underlyingSeries = diffSeries(underlyingSeries, self.diffDays)

        if self.secondDiffDays is not None:
            underlyingSeries = diffSeries(underlyingSeries, self.secondDiffDays)

        if self.volDays is not None:
            underlyingSeries = volSeries(underlyingSeries, self.volDays)
         
        underlyingSeries = rollingAvgSeries(underlyingSeries, self.smoothingDays)

        return underlyingSeries.dropna()

    def checkValidity(self, dseries):
        """
        takes some transformed series and determines if it is invalid over some time range

        :param dseries: modified data

        :returns: modified data

        """
        if pd.DataFrame(dseries).max()[0] == float("inf") or pd.DataFrame(dseries).min()[0] == float("-inf"):
            return False
        else:
            return True
            
    
    def describe(self):
        return (self.ticker, self.diffDays, self.secondDiffDays, self.volDays, self.smoothingDays)

class seriesManager:
    def __init__(self, tickers):
        self.tickers = tickers
    
    def createSeries(self): ##Purpose is either GSEQ or CATALYST
        tickerToUse = self.tickers[random.randint(0, len(self.tickers)) - 1]
        return dataSeries(tickerToUse)
    
    def describe(self):
        return self.tickers


import empyrical
import numpy as np
class walkforwardInputSeries:
    def __init__(self, series, windowSize, predictionLength, targetTicker, lowVolMove):
        self.windowSize = windowSize
        self.series = series
        self.predictionPeriod = predictionLength
        self.targetTicker = targetTicker
        self.lowVolMove = lowVolMove
        print(self.describe())
    
    def generateWindows(self, dataOfInterest):
        ##LOOKBACK ON BASED ON WINDOW day performance of ticker to combine with input arr
            #Return
            #Sharpe Ratio
            #Up Count (how directional is movement)

        transformedData = self.series.transformJoinedData(dataOfInterest)
        xVals = []
        yVals = []
        yIndex = []
        for i in range(len(transformedData) - self.windowSize - self.predictionPeriod):
            inputSeries = transformedData[i:i+self.windowSize]
            lookbackTargetDays = transformedData[i:i+self.windowSize + 1] ##DON'T INCLUDE LAST INDEX SO DO + 1
            lookbackData = dataOfInterest[lookbackTargetDays.index[0]:lookbackTargetDays.index[-1]]

            lookbackDataDaily = getDailyFactorReturn(self.targetTicker, lookbackData)
            factorSR = empyrical.sharpe_ratio(lookbackDataDaily)
            factorVol = empyrical.annual_volatility(lookbackDataDaily)

            ##HOW MUCH TRENDING
            upCount = np.array(lookbackDataDaily.values)
            upCount = len(upCount[upCount > 0])/float(len(upCount))
            # print([factorSR, factorVol, upCount])

            targetDays = transformedData[i+self.windowSize:i+self.windowSize+self.predictionPeriod]
            targetSeries = dataOfInterest["Adj_Close_" + self.targetTicker][targetDays.index]
            transformedTarget = targetSeries.apply(lambda x:(x - targetSeries[0])/targetSeries[0])
            xVals.append(np.array(inputSeries.tolist() + [factorSR, factorVol, upCount])) ##Last 3 Reserverd
            yVals.append(transformedTarget[-1])
            yIndex.append(targetDays.index[0])
        
        xToday = transformedData[-self.windowSize:]
        lookbackData = dataOfInterest[xToday.index[0]:]

        lookbackDataDaily = getDailyFactorReturn(self.targetTicker, lookbackData)
        factorSR = empyrical.sharpe_ratio(lookbackDataDaily)
        factorVol = empyrical.annual_volatility(lookbackDataDaily)

        ##HOW MUCH TRENDING
        upCount = np.array(lookbackDataDaily.values)
        upCount = len(upCount[upCount > 0])/float(len(upCount))



        ##STUFF XVALS WITH LOW MOVEMENT EVENTS

        
        moddedX = xVals
        moddedY = yVals
        moddedYIndex = yIndex
        if self.lowVolMove is not None:
            moddedX = []
            moddedY = []
            moddedYIndex = []
            lowVolMove = np.percentile(np.array([abs(item)for item in yVals]), self.lowVolMove)
            print("LOW VOL MOVE", lowVolMove)
            for i in range(len(xVals)):
                if abs(yVals[i]) > lowVolMove:
                    moddedX.append(xVals[i])
                    moddedY.append(yVals[i]) ##PUT NO EMPHASIS ON SMALL MOVES
                    moddedYIndex.append(yIndex[i])




        return moddedX, moddedY, moddedYIndex, np.array(xToday.tolist() + [factorSR, factorVol, upCount])
    
    def describe(self):
        return (self.windowSize, self.series.describe(), self.predictionPeriod, self.targetTicker)


#Step 2 Build Model
import time
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
import multiprocessing as mp


def computePosition(predictionsArr):
    netPos = 0.0
    for item in predictionsArr:
        if item > 0.51:
            netPos += 1.0
        elif item < 0.49:
            netPos -= 1.0
    return netPos/len(predictionsArr)

def computePositionConfidence(predictionsArr):
    """
    applies a scaling position esimate based on confidence

    :param predictionsArr: some series of predictions

    :returns: scaled predictions

    """
    netPos = 0.0
    for item in predictionsArr:
        netPos += (item - 0.5) * 2
    return netPos/len(predictionsArr)

def applyBinary(predictionsArr):
    """
    applies a simple binary conversion to predictions

    :param predictionsArr: some series of predictions

    :returns: binarized predictions

    """
    return [1.0 if item > 0.0 else -1.0 for item in predictionsArr]

def applyBinarySkipZero(returnArr):
    binaryDailyReturnSeries = []
    for item in returnArr:
        if item > 0.0:
            binaryDailyReturnSeries.append(0.01)
        elif item < 0.0:
            binaryDailyReturnSeries.append(-0.01)

    return np.array(binaryDailyReturnSeries)


def getDailyFactorReturn(ticker, joinedData):
    dailyFactorReturn = joinedData[["Adj_Close_" + ticker]].pct_change(1).shift(-1).dropna()
    dailyFactorReturn.columns = ["Factor Return"]
    return dailyFactorReturn

from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import log_loss, roc_auc_score, accuracy_score
import empyrical

class endToEnd:
    def __init__(self, walkForward, trees, standardization):
        self.walkForward = walkForward
        self.parallelism = 60
        self.treeSize = trees
        self.threshold = None
        self.standardization = standardization
        
    @staticmethod
    def transformTargetArr(targetArr, threshold= None):
        ##IF THRESHOLD IS NONE THEN JUST BINARY
        k = 0
        joinArr = []
        while k < len(targetArr):
            if threshold is None:
                joinArr.append([1.0 if targetArr[k] > 0.0 else 0.0])
            else:
                if targetArr[k] > threshold:
                    joinArr.append(np.array([0, 0, 1]))
                elif targetArr[k] < -threshold:
                    joinArr.append(np.array([1, 0, 0]))
                else:
                    joinArr.append(np.array([0, 1, 0]))
            
            k += 1
                
        
        return joinArr
            
        
        
    def runDay(self, xVals, yVals, xTarget, identifier=None, sharedDict=None):
        
        xSlice = []
        for item in xVals:
            thisTransform = None
            if self.standardization == True:
                scaler = MinMaxScaler()
                thisTransform = scaler.fit_transform(item[:-3]).tolist()  ## because ticker values
                thisTransform += item[-3:].tolist()
            else:
                thisTransform = item
            xSlice.append(np.array(thisTransform))

        xSlice = np.array(xSlice)
        if self.standardization == True:
            scaler = MinMaxScaler()
            xTarget = np.array(scaler.fit_transform(xTarget[:-3]).tolist() + xTarget[-3:].tolist())


        totalModel = ExtraTreesClassifier(self.treeSize, n_jobs=1, 
                                          class_weight="balanced_subsample", 
                                          bootstrap=True) #RandomForestClassifier

        targetY = endToEnd.transformTargetArr(yVals, self.threshold)

        totalModel.fit(
            np.array(xSlice),
            np.array(targetY),
        )
        pred = None
        if self.threshold is None:
            pred = totalModel.predict_proba(np.array([xTarget]))[0][1]
        else:
            pred = np.argmax([item[0][1] for item in totalModel.predict_proba(np.array([xTarget]))]) - 1
            
        
        
        if identifier is not None:
            sharedDict[identifier] = pred
        else:
            return pred
    
    def runDayChunking(self, xVals, yVals, identifiers, sharedDict, k):
        j= 0
        for i in identifiers:
            pred = self.runDay(xVals[:int(i)], yVals[:int(i)], xVals[int(i)+44])
            sharedDict[str(i)] = pred
            j += 1
            if j % 30 == 0:
                print("THREAD ", k, "PROGRESS:", j/len(identifiers))
       
    
    def runModelsChunksSkipMP(self, dataOfInterest, daysToCheck = None):
        xVals, yVals, yIndex, xToday = self.walkForward.generateWindows(dataOfInterest)
        mpEngine = mp.get_context('fork')
        with mpEngine.Manager() as manager:
            returnDict = manager.dict()
            
            identifiersToCheck = []
            
            for i in range(len(xVals) - 44): ##44 is lag...should not overlap with any other predictions or will ruin validity of walkforward optimization
                if i < 600:
                    ##MIN TRAINING
                    continue
                identifiersToCheck.append(str(i))
                
            if daysToCheck is not None:
                identifiersToCheck = identifiersToCheck[-daysToCheck:]


            ##FIRST CHECK FIRST 500 IDENTIFIERS AND THEN IF GOOD CONTINUE
            

            identifierWindows = [identifiersToCheck[:252], identifiersToCheck[252:600], identifiersToCheck[600:900], identifiersToCheck[900:1200], identifiersToCheck[1200:]] ##EXACTLY TWO YEARS
            returnStream = None
            factorReturn = None
            predictions = None
            slippageAdjustedReturn = None
            shortSeen = 0
            for clippedIdentifiers in identifierWindows:
                
                splitIdentifiers = np.array_split(np.array(clippedIdentifiers), 16)
                
                
                runningP = []
                k = 0
                for identifiers in splitIdentifiers:
                    p = mpEngine.Process(target=endToEnd.runDayChunking, args=(self, xVals, yVals, identifiers, returnDict,k))
                    p.start()
                    runningP.append(p)
                    
                    k += 1
                    

                while len(runningP) > 0:
                    newP = []
                    for p in runningP:
                        if p.is_alive() == True:
                            newP.append(p)
                        else:
                            p.join()
                    runningP = newP
                    
                
                preds = []
                actuals = []
                days = []
                for i in clippedIdentifiers:
                    preds.append(returnDict[i])
                    actuals.append(yVals[int(i) + 44])
                    days.append(yIndex[int(i) + 44])

                loss = log_loss(np.array(endToEnd.transformTargetArr(np.array(actuals), self.threshold)), np.array(preds))
                roc_auc = roc_auc_score(np.array(endToEnd.transformTargetArr(np.array(actuals), self.threshold)), np.array(preds))
                accuracy = accuracy_score(np.array(endToEnd.transformTargetArr(np.array(actuals), self.threshold)), np.array(preds).round())
                print(loss, roc_auc, accuracy)
                ##CREATE ACCURATE BLENDING ACROSS DAYS
                predsTable = pd.DataFrame(preds, index=days, columns=["Predictions"])
                i = 1
                tablesToJoin = []
                while i < self.walkForward.predictionPeriod:
                    thisTable = predsTable.shift(i)
                    thisTable.columns = ["Predictions_" + str(i)]
                    tablesToJoin.append(thisTable)
                    i += 1
                predsTable = predsTable.join(tablesToJoin)
                
                transformedPreds = pd.DataFrame(predsTable.apply(lambda x:computePosition(x), axis=1), columns=["Predictions"]).dropna()
                dailyFactorReturn = getDailyFactorReturn(self.walkForward.targetTicker, dataOfInterest)
                transformedPreds = transformedPreds.join(dailyFactorReturn).dropna()
                returnStream = pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"]) if returnStream is None else pd.concat([returnStream, pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"])])
                factorReturn = pd.DataFrame(transformedPreds[["Factor Return"]]) if factorReturn is None else pd.concat([factorReturn, pd.DataFrame(transformedPreds[["Factor Return"]])])
                predictions = pd.DataFrame(transformedPreds[["Predictions"]]) if predictions is None else pd.concat([predictions, pd.DataFrame(transformedPreds[["Predictions"]])])

                alpha, beta = empyrical.alpha_beta(returnStream, factorReturn)
                rawBeta = abs(empyrical.alpha_beta(returnStream.apply(lambda x:applyBinary(x), axis=0), factorReturn.apply(lambda x:applyBinary(x), axis=0))[1])
                shortSharpe = empyrical.sharpe_ratio(returnStream)
                activity = np.count_nonzero(returnStream)/float(len(returnStream))
                algoAnnualReturn = empyrical.annual_return(returnStream.values)[0]
                algoVol = empyrical.annual_volatility(returnStream.values)
                factorAnnualReturn = empyrical.annual_return(factorReturn.values)[0]
                factorVol = empyrical.annual_volatility(factorReturn.values)
                treynor = ((empyrical.annual_return(returnStream.values)[0] - empyrical.annual_return(factorReturn.values)[0]) \
                           / abs(empyrical.beta(returnStream, factorReturn)))
                sharpeDiff = empyrical.sharpe_ratio(returnStream) - empyrical.sharpe_ratio(factorReturn)
                relativeSharpe = sharpeDiff / empyrical.sharpe_ratio(factorReturn) * (empyrical.sharpe_ratio(factorReturn)/abs(empyrical.sharpe_ratio(factorReturn)))
                stability = empyrical.stability_of_timeseries(returnStream)

                ##CALCULATE SHARPE WITH SLIPPAGE
                estimatedSlippageLoss = portfolioGeneration.estimateTransactionCost(predictions)
                estimatedSlippageLoss.columns = returnStream.columns
                slippageAdjustedReturn = (returnStream - estimatedSlippageLoss).dropna()
                slippageSharpe = empyrical.sharpe_ratio(slippageAdjustedReturn)
                sharpeDiffSlippage = empyrical.sharpe_ratio(slippageAdjustedReturn) - empyrical.sharpe_ratio(factorReturn)
                relativeSharpeSlippage = sharpeDiffSlippage / empyrical.sharpe_ratio(factorReturn) * (empyrical.sharpe_ratio(factorReturn)/abs(empyrical.sharpe_ratio(factorReturn)))

                if (empyrical.sharpe_ratio(returnStream) < 0.0 or abs(beta) > 0.7 or activity < 0.5 or accuracy < 0.45) and shortSeen == 0:
                    return None, {
                            "sharpe":shortSharpe, ##OVERLOADED IN FAIL
                            "factorSharpe":empyrical.sharpe_ratio(factorReturn),
                            "sharpeSlippage":slippageSharpe,
                            "beta":abs(beta),
                            "alpha":alpha,
                            "activity":activity,
                            "treynor":treynor,
                            "period":"first 252 days",
                            "algoReturn":algoAnnualReturn,
                            "algoVol":algoVol,
                            "factorReturn":factorAnnualReturn,
                            "factorVol":factorVol,
                            "sharpeDiff":sharpeDiff,
                            "relativeSharpe":relativeSharpe,
                            "sharpeDiffSlippage":sharpeDiffSlippage,
                            "relativeSharpeSlippage":relativeSharpeSlippage,
                            "rawBeta":rawBeta,
                            "stability":stability,
                            "loss":loss,
                            "roc_auc":roc_auc,
                            "accuracy":accuracy
                    }, None, None
                
                elif (((empyrical.sharpe_ratio(returnStream) < 0.25 or slippageSharpe < 0.0) and shortSeen == 1) or ((empyrical.sharpe_ratio(returnStream) < 0.25 or slippageSharpe < 0.0) and (shortSeen == 2 or shortSeen == 3)) or abs(beta) > 0.6 or activity < 0.6 or stability < 0.4  or accuracy < 0.45) and (shortSeen == 1 or shortSeen == 2 or shortSeen == 3):
                    periodName = "first 600 days"
                    if shortSeen == 2:
                        periodName = "first 900 days"
                    elif shortSeen == 3:
                        periodName = "first 1200 days"
                    return None, {
                            "sharpe":shortSharpe, ##OVERLOADED IN FAIL
                            "factorSharpe":empyrical.sharpe_ratio(factorReturn),
                            "sharpeSlippage":slippageSharpe,
                            "alpha":alpha,
                            "beta":abs(beta),
                            "activity":activity,
                            "treynor":treynor,
                            "period":periodName,
                            "algoReturn":algoAnnualReturn,
                            "algoVol":algoVol,
                            "factorReturn":factorAnnualReturn,
                            "factorVol":factorVol,
                            "sharpeDiff":sharpeDiff,
                            "relativeSharpe":relativeSharpe,
                            "sharpeDiffSlippage":sharpeDiffSlippage,
                            "relativeSharpeSlippage":relativeSharpeSlippage,
                            "rawBeta":rawBeta,
                            "stability":stability,
                            "loss":loss,
                            "roc_auc":roc_auc,
                            "accuracy":accuracy
                    }, None, None
                    
                elif shortSeen < 4:
                    print("CONTINUING", "SHARPE:", shortSharpe, "SHARPE DIFF:", sharpeDiff, "RAW BETA:", rawBeta, "TREYNOR:", treynor)
                   
                shortSeen += 1

            return returnStream, factorReturn, predictions, slippageAdjustedReturn
    

    def runModelToday(self, dataOfInterest):
        xVals, yVals, yIndex, xToday = self.walkForward.generateWindows(dataOfInterest)
        return self.runDay(xVals, yVals, xToday, identifier=None, sharedDict=None)
        
from scipy import stats

def vizMLResults(factorReturn, predictions):
    loss = log_loss(np.array(endToEnd.transformTargetArr(factorReturn)), np.array(predictions))
    roc_auc = roc_auc_score(np.array(endToEnd.transformTargetArr(factorReturn)), np.array(predictions))
    accuracy = accuracy_score(np.array(endToEnd.transformTargetArr(factorReturn)), np.array(predictions).round())

    return {
        "LOG LOSS":loss,
        "ROC AUC":roc_auc,
        "ACCURACY":accuracy
    }

def vizResults(slippageAdjustedReturn, returnStream, factorReturn, plotting = False):
    ##ENSURE EQUAL LENGTH
    factorReturn = factorReturn[returnStream.index[0]:] ##IF FACTOR DOES NOT START AT SAME SPOT CAN CREATE VERY SKEWED RESULTS

    ##CALCULATE SHARPE WITH SLIPPAGE
    sharpeDiffSlippage = empyrical.sharpe_ratio(slippageAdjustedReturn) - empyrical.sharpe_ratio(factorReturn)
    relativeSharpeSlippage = sharpeDiffSlippage / empyrical.sharpe_ratio(factorReturn) * (empyrical.sharpe_ratio(factorReturn)/abs(empyrical.sharpe_ratio(factorReturn)))

    alpha, beta = empyrical.alpha_beta(returnStream, factorReturn)
    alphaSlippage, betaSlippage = empyrical.alpha_beta(slippageAdjustedReturn, factorReturn)
    metrics = {"SHARPE": empyrical.sharpe_ratio(returnStream),
               "SHARPE SLIPPAGE":empyrical.sharpe_ratio(slippageAdjustedReturn),
               "STABILITY": empyrical.stability_of_timeseries(returnStream),
               "ALPHA":alpha,
               "ALPHA SLIPPAGE":alphaSlippage,
               "BETA":abs(beta),
               "ANNUALIZED RETURN": empyrical.annual_return(returnStream)[0],
               "ACTIVITY": np.count_nonzero(returnStream)/float(len(returnStream)),
               "TREYNOR": ((empyrical.annual_return(returnStream.values)[0] - empyrical.annual_return(factorReturn.values)[0]) \
                           / abs(empyrical.beta(returnStream, factorReturn))),
               "RAW BETA":abs(empyrical.alpha_beta(returnStream.apply(lambda x:applyBinary(x), axis=0), factorReturn.apply(lambda x:applyBinary(x), axis=0))[1]),
               "SHARPE DIFFERENCE": empyrical.sharpe_ratio(returnStream) - empyrical.sharpe_ratio(factorReturn),
               "RELATIVE SHARPE": (empyrical.sharpe_ratio(returnStream) - empyrical.sharpe_ratio(factorReturn))/empyrical.sharpe_ratio(factorReturn) * (empyrical.sharpe_ratio(factorReturn)/abs(empyrical.sharpe_ratio(factorReturn))),
               "FACTOR SHARPE": empyrical.sharpe_ratio(factorReturn),
               "SHARPE DIFFERENCE SLIPPAGE":sharpeDiffSlippage,
               "RELATIVE SHARPE SLIPPAGE":relativeSharpeSlippage,
              }

    metrics["PROFITABILITY"] = len((returnStream.values)[returnStream.values > 0])/len(returnStream.values)
    metrics["PROFITABILITY SLIPPAGE"] = len((slippageAdjustedReturn.values)[slippageAdjustedReturn.values > 0])/len(slippageAdjustedReturn.values)
    
    metrics["ACTIVE PROFITABILITY"] = len((returnStream.values)[returnStream.values > 0])/len((returnStream.values)[returnStream.values != 0])
    metrics["ACTIVE PROFITABILITY SLIPPAGE"] = len((slippageAdjustedReturn.values)[slippageAdjustedReturn.values > 0])/len((slippageAdjustedReturn.values)[slippageAdjustedReturn.values != 0])

    metrics["TOTAL DAYS SEEN"] = len(returnStream)
    metrics["SHARPE SLIPPAGE DECAY"] = metrics["SHARPE DIFFERENCE SLIPPAGE"] - metrics["SHARPE DIFFERENCE"]
    ##MEASURES BINARY STABILITY OF PREDICTIONS
    metrics["EXTREME STABILITY ROLLING 600"] = (returnStream.rolling(600, min_periods=600).apply(lambda x:empyrical.stability_of_timeseries(applyBinarySkipZero(x)) * (-1 if x[-1] - x[0] < 0 else 1)).dropna()).min().values[0]
    metrics["EXTREME STABILITY"] = empyrical.stability_of_timeseries(applyBinarySkipZero(returnStream.values))
    rollingPeriod = 252


    rollingSharpe = returnStream.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.sharpe_ratio(x)).dropna()
    rollingSharpe.columns = ["252 Day Rolling Sharpe"]
    rollingSharpeFactor = factorReturn.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.sharpe_ratio(x)).dropna()
    rollingSharpe = rollingSharpe.join(rollingSharpeFactor)
    rollingSharpe.columns = ["252 Day Rolling Sharpe Algo", "252 Day Rolling Sharpe Factor"]
    
    if len(rollingSharpe["252 Day Rolling Sharpe Algo"].values) > 50:

        diffSharpe = pd.DataFrame(rollingSharpe.apply(lambda x: x[0] - x[1], axis=1), columns=["Sharpe Difference"])
        metrics["SHARPE DIFFERENCE MIN"] = np.percentile(diffSharpe["Sharpe Difference"].values, 1)
        metrics["SHARPE DIFFERENCE AVERAGE"] = np.percentile(diffSharpe["Sharpe Difference"].values, 50)
        difVals = diffSharpe["Sharpe Difference"].values
        metrics["SHARPE DIFFERENCE GREATER THAN 0"] = len(difVals[np.where(difVals > 0)])/float(len(difVals))
        metrics["25TH PERCENTILE SHARPE DIFFERENCE"] = np.percentile(diffSharpe["Sharpe Difference"].values, 25)
        ###

        relDiffSharpe = pd.DataFrame(rollingSharpe.apply(lambda x: (x[0] - x[1])/x[1] * (x[1]/abs(x[1])), axis=1), columns=["Sharpe Difference"])
        metrics["RELATIVE SHARPE DIFFERENCE MIN"] = np.percentile(relDiffSharpe["Sharpe Difference"].values, 1)
        metrics["RELATIVE SHARPE DIFFERENCE AVERAGE"] = np.percentile(relDiffSharpe["Sharpe Difference"].values, 50)
        relDifVals = relDiffSharpe["Sharpe Difference"].values
        metrics["RELATIVE SHARPE DIFFERENCE GREATER THAN 0"] = len(relDifVals[np.where(relDifVals > 0)])/float(len(relDifVals))
        metrics["25TH PERCENTILE RELATIVE SHARPE DIFFERENCE"] = np.percentile(relDiffSharpe["Sharpe Difference"].values, 25)
        ###
    
        metrics["ROLLING SHARPE BETA"] = abs(empyrical.beta(rollingSharpe["252 Day Rolling Sharpe Algo"], rollingSharpe["252 Day Rolling Sharpe Factor"]))
        metrics["25TH PERCENTILE SHARPE"] = np.percentile(rollingSharpe["252 Day Rolling Sharpe Algo"].values, 25)
        metrics["MIN ROLLING SHARPE"] = np.percentile(rollingSharpe["252 Day Rolling Sharpe Algo"].values, 1)

        rollingDownside = returnStream.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.max_drawdown(x)).dropna()
        rollingDownside.columns = ["252 Day Rolling Downside"]
        rollingDownsideFactor = factorReturn.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.max_drawdown(x)).dropna()
        rollingDownside = rollingDownside.join(rollingDownsideFactor)
        rollingDownside.columns = ["252 Day Rolling Downside Algo", "252 Day Rolling Downside Factor"]

        metrics["ROLLING SHARPE STABILITY"] = abs(stats.linregress(np.arange(len(rollingSharpe["252 Day Rolling Sharpe Algo"].values)),
                                rollingSharpe["252 Day Rolling Sharpe Algo"].values).rvalue)
    

        rollingReturn = returnStream.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.cum_returns(x)[-1]).dropna()
        rollingReturn.columns = ["ROLLING RETURN"]
        metrics["SMART INFORMATION RATIO"] = (np.percentile(rollingReturn["ROLLING RETURN"].values, 25) - empyrical.annual_return(factorReturn.values[0]))\
                        / returnStream.values.std()

        metrics["ROLLING SHARPE ERROR"] = rollingSharpe["252 Day Rolling Sharpe Algo"].std()
        metrics["ONE STD SHARPE"] = empyrical.sharpe_ratio(slippageAdjustedReturn) - metrics["ROLLING SHARPE ERROR"]
        if plotting == True:
            import matplotlib.pyplot as plt 
            rollingSharpe.plot()
            rollingDownside.plot()

    rollingPeriod = 90


    rollingSharpe = returnStream.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.sharpe_ratio(x)).dropna()
    rollingSharpe.columns = ["90 Day Rolling Sharpe"]

    if len(rollingSharpe["90 Day Rolling Sharpe Algo"].values) > 50:

        metrics["25TH PERCENTILE SHARPE 90"] = np.percentile(rollingSharpe["90 Day Rolling Sharpe Algo"].values, 25)
        metrics["MIN ROLLING SHARPE 90"] = np.percentile(rollingSharpe["90 Day Rolling Sharpe Algo"].values, 1)
        metrics["ROLLING SHARPE ERROR 90"] = rollingSharpe["90 Day Rolling Sharpe Algo"].std()

    
    returns = returnStream.apply(lambda x:empyrical.cum_returns(x))
    returns.columns = ["algo"]
    factorReturn = factorReturn.apply(lambda x:empyrical.cum_returns(x))
    returns = returns.join(factorReturn)
    returns.columns = ["Algo Return", "Factor Return"]


        ##FORCE SHOW
    if plotting == True:
        import matplotlib.pyplot as plt 
        returns.plot()
        plt.show()
    return metrics

class algoBlob:
    def __init__(self, inputSeries, windowSize, trees, predictionLength, standardization, lowVolMove, targetTicker):
        self.inputSeries = walkforwardInputSeries(inputSeries, windowSize, predictionLength, targetTicker, lowVolMove)
        self.windowSize = windowSize
        self.trees = trees
        self.standardization = standardization
        self.lowVolMove = lowVolMove
        self.e2e = endToEnd(self.inputSeries, trees, standardization)
        print("SERIES", self.inputSeries.describe(), "WINDOW", windowSize, "TREES", trees)
    
    def makePredictions(self, dataOfInterest, daysToCheck = None, earlyStop = False):
        #algoReturn, factorReturn, predictions 
        if earlyStop == False: 
            return self.e2e.runModelsCHUNKINGMP(dataOfInterest, daysToCheck)
        else:
            return self.e2e.runModelsChunksSkipMP(dataOfInterest, daysToCheck)
    
    def makeTodayPrediction(self, dataOfInterest):
        return self.e2e.runModelToday(dataOfInterest)
    
    def describe(self):
        return (self.inputSeries.describe(), self.windowSize, self.trees, self.standardization, self.lowVolMove)

def logModel(topic, message):
    while True:
        try:
            loggingClient = logging.Client('money-maker-1236')
            logger = loggingClient.logger(topic.replace(" ", ""))
            logger.log_struct(message)
            break
        except:
            time.sleep(10)
            print("LOGGING ERROR:", str(sys.exc_info()))

def storeModel(model, trainingMetrics, oosMetrics):
    toUpload = trainingMetrics
    for k in oosMetrics:
        toUpload["OOS_" + k] = oosMetrics[k]
    toUpload["ticker"] = model.inputSeries.targetTicker
    toUpload["predictionLength"] = model.inputSeries.predictionPeriod
    toUpload["trees"] = model.trees
    toUpload["windowSize"] = model.windowSize
    toUpload["model"] = pickle.dumps(model)
    toUpload["standardization"] = model.standardization
    toUpload["lowVolMove"] = model.lowVolMove
    organismHash = hashlib.sha224(str(model.describe()).encode('utf-8')).hexdigest()
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            key = datastoreClient.key(params.datastoreName,  organismHash) #NEED TO HASH TO ENSURE UNDER COUNT
            organismToStore = datastore.Entity(key=key, exclude_from_indexes=["model"])
            organismToStore.update(toUpload)
            datastoreClient.put(organismToStore)
            break
        except:
            print("UPLOAD ERROR:", str(sys.exc_info()))
            time.sleep(10)
    
    ##LOG SUCCESSFUL STORE
    toLog = {}
    for item in toUpload:
        if item != "model":
            toLog[item] = toUpload[item]
        else:
            toLog[item] = str(model.describe())
    logModel("StoredModel", toLog)

    
def storeTrainingData(ticker, joinedData):
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(params.trainingDataCache)
            blob = storage.Blob(ticker, bucket)
            blob.upload_from_string(pickle.dumps(joinedData))
            print("STORING", ticker)
            break
        except:
            print("UPLOAD BLOB ERROR:", str(sys.exc_info()))
            time.sleep(10)
    pass

def getTrainingData(ticker):
    storageClient = storage.Client('money-maker-1236')
    try:
        bucket = storageClient.get_bucket(params.trainingDataCache)
        print("ATTEMPTING PULL", ticker)
        blob = storage.Blob(ticker, bucket)
        return pickle.loads(blob.download_as_string())
    except:
        return None
    pass


def storeModelData(model, algoReturns, algoPredictions, algoReturnsSlippage):
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(params.modelDataCache)
            organismHash = hashlib.sha224(str(model.describe()).encode('utf-8')).hexdigest()
            blob = storage.Blob(organismHash, bucket)
            blob.upload_from_string(pickle.dumps((algoReturns, algoPredictions, algoReturnsSlippage)))
            print("STORING", organismHash)
            break
        except:
            print("UPLOAD BLOB ERROR:", str(sys.exc_info()))
            time.sleep(10)
    pass

def getModelData(model):
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(params.modelDataCache)
            organismHash = hashlib.sha224(str(model.describe()).encode('utf-8')).hexdigest()
            print("ATTEMPTING PULL", organismHash)
            blob = storage.Blob(organismHash, bucket)
            return pickle.loads(blob.download_as_string())
        except:
            return None
    pass
    

