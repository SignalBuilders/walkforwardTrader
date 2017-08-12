# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
import logging
import misc
from flask import Flask, render_template
import time
import json
app = Flask(__name__)


@app.route('/')
def viewPortfolios():
    """Return a list of all portfolios."""

    return render_template('portfolios.html', 
        availablePortfolios=misc.getPortfolios())

@app.route('/<portfolio>')
def viewPortfolio(portfolio=None):
    """Return information about portfolio."""

    ##CONVERT ALLOCATIONS TO GRAPHABLE DATA
    allAllocations = misc.getPortfolioAllocations(portfolio)
    rows = []
    columns = []

    parsedAllocations = {}
    for allocation in allAllocations:
        thisRow = []
        tickerVals = {}
        algoVals = {}
        for key in allocation:
            if key[:7] == "ticker_":
                tickerVals[key[7:]] = allocation[key]
                if key[7:] not in columns:
                    columns.append(str(key[7:]))
            # if key[:5] == "algo_":
            #     algoVals[key[5:]] = allocation[key]
        timestamp = int(time.mktime(allocation['lastDataDayUsed'].timetuple())) * 1000
        thisRow.append(timestamp)
        for item in columns:
            thisRow.append(tickerVals[item])

        rows.append(thisRow)
    days = []
    tickerAllocations = []
    return render_template('viewPortfolio.html', portfolio=json.dumps(rows, separators=(',', ': ')), 
        columns=json.dumps(columns, separators=(',', ': ')))


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END app]