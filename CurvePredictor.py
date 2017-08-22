from sklearn.neighbors import NearestNeighbors
import multiprocessing as mp
import empyrical
import portfolioGeneration
import pandas as pd
import numpy as np
import dataAck
import portfolio
from sklearn.preprocessing import MinMaxScaler
class CurvePredictor:
    def __init__(self, inputSeries, targetTicker, lookbackDistance, predictionDistance, radius, minConfidence, minNeighbors):
        self.parallelism = 16
        self.inputSeries = inputSeries
        self.targetTicker = targetTicker
        self.lookbackDistance = lookbackDistance
        self.predictionDistance = predictionDistance
        self.radius = radius
        self.minConfidence = minConfidence
        self.minNeighbors = minNeighbors
    
    def describe(self):
        return (self.inputSeries.describe(), self.targetTicker, self.lookbackDistance, self.predictionDistance, self.radius, self.minConfidence, self.minNeighbors)

    @staticmethod
    def ensureNoShifts(nearestIndicies):
        breadth = 5
        keptIndicies = []
        for item in nearestIndicies:
            k = item-breadth
            shouldAdd = True
            while k < item + breadth:
                if k in keptIndicies:
                    shouldAdd = False
                    break
                k += 1
            if shouldAdd == True:
                keptIndicies.append(item)
        return keptIndicies
    
    def generateWindows(self, joinedData):
        transformedSeries = pd.DataFrame(self.inputSeries.transformJoinedData(joinedData))
        transformedSeries.columns = ["INPUT"]
        targetSeries = pd.DataFrame(dataAck.createPriceSeries(joinedData, self.targetTicker))
        targetSeries.columns = ["OUTPUT"]
        
        joinedTrain = targetSeries.join(transformedSeries).dropna() ##DONE TO ENSURE SAME INDEXES
        
        inputData = joinedTrain[["INPUT"]].values
        outputData = joinedTrain[["OUTPUT"]].values
        outputDays = joinedTrain[["OUTPUT"]].index
        
        xVals = []
        yVals = []
        yIndex = []
        for i in range(len(inputData) - self.lookbackDistance - self.lookbackDistance - self.predictionDistance):
            xVals.append(MinMaxScaler().fit_transform(inputData[i:i+self.lookbackDistance]).flatten())
            ##SKIP LOOKBACK DISTANCE * 2 TO AVOID ANY OVERLAP WITH ANYTHING IN TRAINING
            targetArr = outputData[i+self.lookbackDistance + self.lookbackDistance:i+self.lookbackDistance+self.lookbackDistance+self.predictionDistance]
            daysArr = outputDays[i+self.lookbackDistance + self.lookbackDistance:i+self.lookbackDistance+self.lookbackDistance+self.predictionDistance]
            yVals.append((targetArr[-1] - targetArr[0])/targetArr[0])
            yIndex.append(daysArr[0])
        return xVals, yVals, yIndex, MinMaxScaler().fit_transform(transformedSeries[-self.lookbackDistance:]).flatten()
    
    def runDay(self, xVals, yVals, xTarget, identifier=None, sharedDict=None):
        
        
        nn = NearestNeighbors(p=2, n_jobs = 1)
        nn.fit(xVals)
        closest = nn.radius_neighbors([xTarget], self.radius)
        keptNeighbors = CurvePredictor.ensureNoShifts(closest[1][0])
        pred = 0.5
        if len(keptNeighbors) > self.minNeighbors:
            predictions = []
            for sampleIndex in keptNeighbors:
                predictions.append(yVals[sampleIndex])
            predictions = np.array(predictions)

            pred = len(predictions[predictions > 0])/float(len(predictions))
        if abs(pred - 0.5) < self.minConfidence:
            pred = 0.5

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
        xVals, yVals, yIndex, xToday = self.generateWindows(dataOfInterest)
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
                    p = mpEngine.Process(target=CurvePredictor.runDayChunking, args=(self, xVals, yVals, identifiers, returnDict,k))
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

                ##CREATE ACCURATE BLENDING ACROSS DAYS
                predsTable = pd.DataFrame(preds, index=days, columns=["Predictions"])
                i = 1
                tablesToJoin = []
                while i < self.predictionDistance:
                    thisTable = predsTable.shift(i)
                    thisTable.columns = ["Predictions_" + str(i)]
                    tablesToJoin.append(thisTable)
                    i += 1
                
                predsTable = predsTable.join(tablesToJoin)
                
                transformedPreds = pd.DataFrame(predsTable.apply(lambda x:dataAck.computePosition(x), axis=1), columns=["Predictions"]).dropna()
                dailyFactorReturn = dataAck.getDailyFactorReturn(self.targetTicker, dataOfInterest)
                transformedPreds = transformedPreds.join(dailyFactorReturn).dropna()
                returnStream = pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"]) if returnStream is None else pd.concat([returnStream, pd.DataFrame(transformedPreds.apply(lambda x:x[0] * x[1], axis=1), columns=["Algo Return"])])
                factorReturn = pd.DataFrame(transformedPreds[["Factor Return"]]) if factorReturn is None else pd.concat([factorReturn, pd.DataFrame(transformedPreds[["Factor Return"]])])
                predictions = pd.DataFrame(transformedPreds[["Predictions"]]) if predictions is None else pd.concat([predictions, pd.DataFrame(transformedPreds[["Predictions"]])])

                alpha, beta = empyrical.alpha_beta(returnStream, factorReturn)
                rawBeta = abs(empyrical.alpha_beta(returnStream.apply(lambda x:dataAck.applyBinary(x), axis=0), factorReturn.apply(lambda x:dataAck.applyBinary(x), axis=0))[1])
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
                if np.isnan(shortSharpe) == True:
                    return None, {"sharpe":shortSharpe}, None, None

                elif (empyrical.sharpe_ratio(returnStream) < 0.0 or abs(beta) > 0.7) and shortSeen == 0:
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
                            "stability":stability
                    }, None, None
                
                elif (((empyrical.sharpe_ratio(returnStream) < 0.25) and shortSeen == 1) or ((empyrical.sharpe_ratio(returnStream) < 0.25 or sharpeDiff < 0.0) and (shortSeen == 2 or shortSeen == 3)) or abs(beta) > 0.6) and (shortSeen == 1 or shortSeen == 2 or shortSeen == 3):
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
                            "stability":stability
                    }, None, None
                    
                elif shortSeen < 4:
                    print("CONTINUING", "SHARPE:", shortSharpe, "SHARPE DIFF:", sharpeDiff, "RAW BETA:", rawBeta, "TREYNOR:", treynor)
                   
                shortSeen += 1

            return returnStream, factorReturn, predictions, slippageAdjustedReturn
    

    def runModelToday(self, dataOfInterest):
        xVals, yVals, yIndex, xToday = self.generateWindows(dataOfInterest)
        return self.runDay(xVals, yVals, xToday, identifier=None, sharedDict=None)
        