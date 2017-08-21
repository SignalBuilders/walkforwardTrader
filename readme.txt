##ORDER OF COMPUTATION

-> modelSearch.py

Model search iterates through different series and different learning combinations and stores results in params.datastoreName

-> modelResultCacher.py

Saves and backfills aggregate model predictions

	-> automaticPortfolioGenerator.py

	Automatically creates random portfolios of algorithms

	-> [Manual] Manual Portfolio Generation.ipynb

-> [Once Required] Upgrade Portfolio to Trading Portfolios.ipynb

Move a discovered portfolio to paper trading or real trading

-> [Each Day] dailyModelAndPortfolioUpdate.py

Update all models and portfolios saved with daily predictions...results saved in params.status and displayed on website when logo is clicked.


	-> Generate Net Position From Trading Portfolios and Save [Done only when trading algorithms]
