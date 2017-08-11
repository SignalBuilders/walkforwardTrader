import portfolio
import dataAck

allModels = portfolio.getModels()
tickersRequired = []
for mod in allModels:
    print(mod.describe())
    if mod.inputSeries.targetTicker not in tickersRequired:
        tickersRequired.append(mod.inputSeries.targetTicker)


##ITEMS REQUIRED TO DOWNLOAD
allDataRequired = []
for tick in tickersRequired:
    for item in dataAck.getDataSourcesForTicker(tick):
        if item not in allDataRequired:
            allDataRequired.append(item)

pulledData, validTickers = dataAck.downloadTickerData(allDataRequired)

joinedData = dataAck.joinDatasets([pulledData[ticker] for ticker in pulledData])

aggregateReturns = portolio.generateAggregateReturns(allModels, joinedData)

hrpReturns, weights = produceHRPPredictions(aggregateReturns, 22, True)

print(weights)

##MAKE TODAY PREDICTION
for mod in allModels:
    pred = dataAck.computePosition([mod.makeTodayPrediction(joinedData)])
    print(mod.describe(), pred, pred * weights[str(mod.describe())])
    portfolio.storeModelPrediction(mod, pred * weights[str(mod.describe())], joinedData.index[-1])

##TAKE WEIGHTS FROM PORTFOLIO
for ticker in tickersRequired:
    netPosition = 0.0
    for pred in portfolio.getModelPrediction(ticker):
        ##CHECK IF PREDICTION STILL VALID
        if len(joinedData[str(pred["lastDataDayUsed"]):]) - 1 < pred["predictionLength"]:##GETS TRADING DAYS SINCE LAST DATA DAY
            ##MUST DO -1 BECAUSE INCLUDES DAY LAST USED IN LEN
#             print(pred["ticker"], pred["prediction"])
            netPosition += pred["prediction"]
    print(ticker, netPosition)