import dataAck
import portfolio
import CurvePredictor
import TreePredictor
import FactorPredictor
import curveTreeDB
import params
import empyrical
import datetime 
from google.cloud import error_reporting


allTickers = dataAck.getAllTickersPlain()

##SHOULD BE PRE-INITIALIZED
tData = dataAck.getTrainingData(params.tickerDataLookup)
joinedData = tData[0]
validTickers = tData[1]
print("TICKER", "ANNUALIZED RETURN", "ANNUALIZED VOLATILITY", "COST OF RETURN")
for tickerToTrade in validTickers:
    if tickerToTrade != "VTI" and tickerToTrade != "SPY" and tickerToTrade != "AGG"\
         and tickerToTrade != "GLD" and tickerToTrade != "JNK" and tickerToTrade != "USO"\
         and tickerToTrade != "EEM":
        continue
    for pos in [1.0]:
        fPre = FactorPredictor.FactorPredictor(tickerToTrade, pos)
        algoReturn, factorReturn, predictions, slippageAdjustedReturn, rawPredictions = fPre.runModelHistorical(joinedData)
        print(tickerToTrade, round(empyrical.annual_return(factorReturn)[0] * 100, 2), "%",\
                     round(empyrical.annual_volatility(factorReturn) * 100, 2), "%", \
                     round(empyrical.annual_return(factorReturn)[0] * 100, 2) - round(empyrical.annual_volatility(factorReturn) * 100, 2), "%")
