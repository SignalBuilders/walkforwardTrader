

##USED TO STORE DESCRIPTION INFO
def storeModel(db, model, uploadInformation, trainingMetrics, oosMetrics):
    toUpload = uploadInformation
    for k in trainingMetrics:
        toUpload["IS_" + k] = trainingMetrics[k]
    for k in oosMetrics:
        toUpload["OOS_" + k] = oosMetrics[k]
    toUpload["model"] = pickle.dumps(model)
    organismHash = model.getHash()
    ##UPLOAD ORGANISM OBJECT
    while True:
        try:
            datastoreClient = datastore.Client('money-maker-1236')
            #HASH DIGEST
            key = datastoreClient.key(db,  organismHash) #NEED TO HASH TO ENSURE UNDER COUNT
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
    logModel("StoredModel"+"_" + db, toLog)

def getModels(db, ticker = None, returnEntireObject = False):
    while True:
        try:
            datastore_client = datastore.Client('money-maker-1236')
            query = datastore_client.query(kind=db)
            if ticker is not None:
                query.add_filter('ticker', '=', ticker)
            retrievedModels = list(query.fetch())
            toReturn = []
            for source in retrievedModels:
                if returnEntireObject == False:
                    toReturn.append(pickle.loads(source["model"]))
                else:
                    source["model"] = pickle.loads(source["model"])
                    toReturn.append(source)
            return toReturn
        except:
            time.sleep(10)
            print("DATA SOURCE RETRIEVAL ERROR:", str(sys.exc_info()))


##USED TO STORE PREDICTIONS -> USEFUL FOR PORTFOLIO CONSTRUCTION...ONLY DO FOR TREE PREDICTOR
def storeModelData(db, model, algoReturns, algoPredictions, algoReturnsSlippage):
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(db)
            organismHash = model.getHash()
            blob = storage.Blob(organismHash, bucket)
            blob.upload_from_string(pickle.dumps((algoReturns, algoPredictions, algoReturnsSlippage)))
            print("STORING", organismHash)
            break
        except:
            print("UPLOAD BLOB ERROR:", str(sys.exc_info()))
            time.sleep(10)
    pass

def getModelData(db, model):
    storageClient = storage.Client('money-maker-1236')
    while True:
        try:
            bucket = storageClient.get_bucket(db)
            organismHash = model.getHash()
            print("ATTEMPTING PULL", organismHash)
            blob = storage.Blob(organismHash, bucket)
            return pickle.loads(blob.download_as_string())
        except:
            return None
    pass