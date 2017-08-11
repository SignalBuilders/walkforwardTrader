##USE QUANDL AND PANDAS
import quandl
import pandas as pd

def getTickerData(ticker):
    ##RETURNS A DATAFRAME WITH ONLY THE COLUMNS WE CARE ABOUT
    limitedDf = quandl.get(["EOD/" + ticker +".11"], authtoken="G3AvFe4ZUZoBEthhjmEY")
    limitedDf.columns = ["Adj_Close_" + ticker]
    return limitedDf


import time
import multiprocessing as mp
import sys
def cycleP(runningProcesses):
    newP = []
    for p in runningProcesses:
        if p.is_alive() == True:
            newP.append(p)
        else:
            p.join()
    return newP

def downloadTickerData(storedTickers):
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
    baseTicker = tickerDatas[0]
    for ticker in tickerDatas[1:]:
        baseTicker = baseTicker.join(ticker, how='outer').dropna()
    return baseTicker



import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import numpy as np

def getAllTickersPlain():
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



##SERIES MANIPULATION

def createPriceSeries(joinedData, ticker):
    return joinedData["Adj_Close_" + ticker]


def diffSeries(priceVector, diffAmount):
    return (priceVector.diff(diffAmount)/priceVector).dropna()

def volSeries(priceVector, lookback):
    return (priceVector.rolling(window=lookback, center=False).std()).dropna()

import random
class dataSeries:
    def __init__(self, ticker):
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
        
        
    def transformJoinedData(self, joinedData):
        underlyingSeries = createPriceSeries(joinedData, self.ticker)

        if self.diffDays is not None:
            underlyingSeries = diffSeries(underlyingSeries, self.diffDays)

        if self.secondDiffDays is not None:
            underlyingSeries = diffSeries(underlyingSeries, self.secondDiffDays)

        if self.volDays is not None:
            underlyingSeries = volSeries(underlyingSeries, self.volDays)
 

        return underlyingSeries.dropna()

    def checkValidity(self, dseries):
        if pd.DataFrame(dseries).max()[0] == float("inf") or pd.DataFrame(dseries).min()[0] == float("-inf"):
            return False
        else:
            return True
            
    
    def describe(self):
        return (self.ticker, self.diffDays, self.secondDiffDays, self.volDays)

class seriesManager:
    def __init__(self, tickers):
        self.tickers = tickers
    
    def createSeries(self): ##Purpose is either GSEQ or CATALYST
        tickerToUse = self.tickers[random.randint(0, len(self.tickers)) - 1]
        return dataSeries(tickerToUse)
    
    def describe(self):
        return self.tickers



import numpy as np
class walkforwardInputSeries:
    def __init__(self, series, windowSize, predictionLength, targetTicker):
        self.windowSize = windowSize
        self.series = series
        self.predictionPeriod = predictionLength
        self.targetTicker = targetTicker
        print(self.describe())
    
    def generateWindows(self, dataOfInterest):
        transformedData = self.series.transformJoinedData(dataOfInterest)
        xVals = []
        yVals = []
        yIndex = []
        for i in range(len(transformedData) - self.windowSize - self.predictionPeriod):
            inputSeries = transformedData[i:i+self.windowSize]
            targetDays = transformedData[i+self.windowSize:i+self.windowSize+self.predictionPeriod]
            targetSeries = dataOfInterest["Adj_Close_" + self.targetTicker][targetDays.index]
            transformedTarget = targetSeries.apply(lambda x:(x - targetSeries[0])/targetSeries[0])
            xVals.append(np.array(inputSeries))
            yVals.append(transformedTarget[-1])
            yIndex.append(targetDays.index[0])
        return xVals, yVals, yIndex, transformedData[-self.windowSize:]
    
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

def applyBinary(predictionsArr):
    return [1.0 if item > 0.0 else -1.0 for item in predictionsArr]


def getDailyFactorReturn(ticker, joinedData):
    dailyFactorReturn = joinedData[["Adj_Close_" + ticker]].pct_change(1).shift(-1).dropna()
    dailyFactorReturn.columns = ["Factor Return"]
    return dailyFactorReturn

from sklearn.preprocessing import StandardScaler
import empyrical

class endToEnd:
    def __init__(self, walkForward, trees):
        self.walkForward = walkForward
        self.parallelism = 60
        self.treeSize = trees
        self.threshold = None
        
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
        scaler = StandardScaler()
        realArr = []
        for item in xVals:
            realArr.append(item[-1])
        scaler.fit(realArr)

        xSlice = [scaler.transform(item) for item in xVals]

        xTarget = scaler.transform(xTarget)

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
       
    
    
    def runModelsCHUNKINGMP(self, dataOfInterest, daysToCheck = None):
        xVals, yVals, yIndex, xToday = self.walkForward.generateWindows(dataOfInterest)
        mpEngine = mp.get_context('fork')
        with mpEngine.Manager() as manager:
            returnDict = manager.dict()
            
            identifiersToCheck = []
            
            for i in range(len(xVals) - 44): ##44 is lag 
                if i < 600:
                    ##MIN TRAINING
                    continue
                identifiersToCheck.append(str(i))
                
            if daysToCheck is not None:
                identifiersToCheck = identifiersToCheck[-daysToCheck:]
                
            splitIdentifiers = np.array_split(np.array(identifiersToCheck), self.parallelism)
            
            
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
            factorReturn = []
            returnStream = []
            days = []
            for i in identifiersToCheck:
                preds.append(returnDict[i])
                actuals.append(yVals[int(i) + 44])
                days.append(yIndex[int(i) + 44])
                
            ##CREATE ACCURATE BLENDING ACROSS DAYS
            predsTable = pd.DataFrame(preds, index=days, columns=["Predictions"])
            i = 1
            while i < self.walkForward.predictionPeriod:
                predsTable = predsTable.join(predsTable.shift(i), rsuffix="_" + str(i))
                i += 1
            
            transformedPreds = pd.DataFrame(predsTable.apply(lambda x:computePosition(x), axis=1), columns=["Predictions"]).dropna()
            dailyFactorReturn = getDailyFactorReturn(self.walkForward.targetTicker, dataOfInterest)
            transformedPreds = transformedPreds.join(dailyFactorReturn).dropna()
            returnStream = pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"])
            factorReturn = pd.DataFrame(transformedPreds[["Factor Return"]])
            predictions = pd.DataFrame(transformedPreds[["Predictions"]])
            
            return returnStream, factorReturn, predictions
    

    def runModelToday(self, dataOfInterest):
        xVals, yVals, yIndex, xToday = self.walkForward.generateWindows(dataOfInterest)
        return self.runDay(xVals, yVals, xToday, identifier=None, sharedDict=None)
        
from scipy import stats

def vizResults(returnStream, factorReturn, plotting = False):
    alpha, beta = empyrical.alpha_beta(returnStream, factorReturn)
    metrics = {"SHARPE": empyrical.sharpe_ratio(returnStream),
               "STABILITY": empyrical.stability_of_timeseries(returnStream),
               "ALPHA":alpha,
               "BETA":abs(beta),
               "ANNUALIZED RETURN": empyrical.annual_return(returnStream)[0],
               "ACTIVITY": np.count_nonzero(returnStream)/float(len(returnStream)),
               "TREYNOR": ((empyrical.annual_return(returnStream.values)[0] - empyrical.annual_return(factorReturn.values)[0]) \
                           / abs(empyrical.beta(returnStream, factorReturn))),
               "RAW BETA":abs(empyrical.alpha_beta(returnStream.apply(lambda x:applyBinary(x), axis=0), factorReturn.apply(lambda x:applyBinary(x), axis=0))[1])
                  
              }
    rollingPeriod = 252
    
    rollingSharpe = returnStream.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.sharpe_ratio(x)).dropna()
    rollingSharpe.columns = ["252 Day Rolling Sharpe"]
    rollingSharpeFactor = factorReturn.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.sharpe_ratio(x)).dropna()
    rollingSharpe = rollingSharpe.join(rollingSharpeFactor)
    rollingSharpe.columns = ["252 Day Rolling Sharpe Algo", "252 Day Rolling Sharpe Factor"]
    
    if len(rollingSharpe["252 Day Rolling Sharpe Algo"].values) > 50:
    
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
    
        metrics["TREYNOR+"] = ((empyrical.annual_return(returnStream.values)[0] - empyrical.annual_return(factorReturn.values)[0]) \
                               / (abs(empyrical.beta(returnStream, factorReturn)))) * abs(metrics["ROLLING SHARPE STABILITY"])


        rollingReturn = returnStream.rolling(rollingPeriod, min_periods=rollingPeriod).apply(lambda x:empyrical.cum_returns(x)[-1]).dropna()
        rollingReturn.columns = ["ROLLING RETURN"]
        metrics["SMART INFORMATION RATIO"] = (np.percentile(rollingReturn["ROLLING RETURN"].values, 25) - empyrical.annual_return(factorReturn.values[0]))\
                        / returnStream.values.std()

        metrics["ROLLING SHARPE ERROR"] = rollingSharpe["252 Day Rolling Sharpe Algo"].std()
        if plotting == True:
            import matplotlib.pyplot as plt 
            rollingSharpe.plot()
            rollingDownside.plot()

    
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
    def __init__(self, inputSeries, windowSize, trees, predictionLength, targetTicker):
        self.inputSeries = walkforwardInputSeries(inputSeries, windowSize, predictionLength, targetTicker)
        self.windowSize = windowSize
        self.trees = trees
        self.e2e = endToEnd(self.inputSeries, trees)
        print("SERIES", self.inputSeries.describe(), "WINDOW", windowSize, "TREES", trees)
    
    def makePredictions(self, dataOfInterest, daysToCheck = None):
        #algoReturn, factorReturn, predictions 
        return self.e2e.runModelsCHUNKINGMP(dataOfInterest, daysToCheck)
    
    def makeTodayPrediction(self, dataOfInterest):
        return self.e2e.runModelToday(dataOfInterest)
    
    def describe(self):
        return (self.inputSeries.describe(), self.windowSize, self.trees)

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
    toUpload["model"] = pickle.dumps(model)
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
    

