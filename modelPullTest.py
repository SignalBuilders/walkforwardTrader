import params
from google.cloud import datastore, storage, logging
import time
import pickle
import hashlib
import sys
import time
import multiprocessing as mp
import sys
import dataAck
import curveTreeDB


print("CURVES", curveTreeDB.getValidCounts(params.curveModels))

print("TREES", curveTreeDB.getValidCounts(params.treeModels))
