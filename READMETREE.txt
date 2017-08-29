##PROCESS FOR TREE

##ORDER OF COMPUTATION

-> prepareTickerData

store all data in cloud bucket so don't need to hit quandl endpoint

prior to this strategy change, only tickers with a beta lower than 0.5 could influence [normal beta, not binary...so very volatile tickers could use anything and non-volatile had 4 or 5 possible sources]

-> curveAndTreeSearch.py

Model search iterates through different series and different learning combinations and stores results for both
generated curves and trees

-> modelResultCacherTree.py

Saves and backfills aggregate model predictions

	-> cacheValidModels...saves a lot of database calls

	-> automaticPortfolioGeneratorTree.py

	Automatically creates random portfolios of algorithms


-> [Once Required] TODO Upgrade Portfolio to Trading Portfolios Tree.ipynb

Move a discovered portfolio to paper trading or real trading

-> [Each Day] dailyTreeUpdate.py

Update all models and portfolios saved with daily predictions...results saved in params.status and displayed on website when logo is clicked.


	-> Generate Net Position From Trading Portfolios and Save [Done only when trading algorithms] ##TODO