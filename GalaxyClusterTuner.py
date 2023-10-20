import trino
import pandas as pd
import json
import requests
import base64
import time
import datetime
from pathlib import Path
import sys
import pytz

def issueQuery(query, resultsRequested):
    # open the cursor
    rows = None

    cur = trino_conn.cursor()
    # execute the a simple query
    try:
        cur.execute(query)
        rows = cur.fetchall()
        print("query: {0}, stats: {1}".format(query, cur.stats))

    except Exception as e:
        print(e)
    
    if(resultsRequested):
    # fetch all the rows. This will create a list type
        cur.close()
        print("Returning Results")
        return rows
    
    stats = cur.stats
    rows = None
    #close the cursor
    cur.close()
    return stats

#Issue Actual Query
def Query(query):
    getStats = issueQuery(query, False)
    #results = issueQuery('SELECT * FROM system.runtime.queries where source = \'trino-python-client\' and query_id = ' + "'" + getStats['queryId'] + "'", True)
    results = issueQuery('SELECT query_id FROM system.runtime.queries where source = \'trino-python-client\' and query_id = ' + "'" + getStats['queryId'] + "'", True)

    return results
  
class GalaxyDomain:
    
    def getBearerToken(self):

        auth_endpoint = "https://" + self.domain + "/oauth/v2/token"

        headers = {
            'Authorization': 'Basic ' + self.auth,
            'Content-Type': 'application/x-www-form-urlencoded'
            }
        data = {'grant_type': 'client_credentials'}

        response = requests.post(auth_endpoint, data=data, headers=headers)

        #**Not sure this actually works - you may need to return it instead of setting it directly here
        try:
            self.bearerToken, self.bearerTokenExpiry = response.json()['access_token'], time.time() + response.json()['expires_in']
        except Exception as e:
            print(e)
            print("Error getting bearerToken - check your credentials")
            self.bearerToken, self.bearerTokenExpiry = None, None
            
    #Find the Test Cluster
    def findCluster(self):

        for currentCluster in self.allClusters:
            if "trinoUri" in currentCluster.keys():
                if currentCluster['trinoUri'] == 'https://' + self.clusterName + ':443':
                    print(f"ClusterName: {self.clusterName}, minWorkers: {currentCluster['minWorkers']}, maxWorkers: {currentCluster['maxWorkers']}, batchMode: {currentCluster['batchCluster']}, warpSpeedCluster: {currentCluster['warpResiliencyEnabled']}")
                    finalCluster = currentCluster
        return finalCluster

    #Set the ClusterID of the Test Cluster
    def setClusterID(self):
        
        clusterDetails = self.findCluster()
        return clusterDetails['clusterId']


    #Return all the clusters in the account
    def listCluster(self):
        
        if(time.time() > self.bearerTokenExpiry):
            print('Auth Token expired, getting a new one!')
            self.getBearerToken()

        endpoint = "https://" + self.domain + "/public/api/v1/cluster"

        headers = {
            'Authorization': 'Bearer ' + self.bearerToken,
            'Content-Type': 'application/x-www-form-urlencoded'
            }
        return requests.get(endpoint, headers=headers).json()['result']

    #Get cluster by id
    def getCluster(self):

        if(time.time() > self.bearerTokenExpiry):
            print('Auth Token expired, getting a new one!')
            self.getBearerToken()

        endpoint = "https://" + self.domain + "/public/api/v1/cluster/" + self.clusterID
        
        headers = {
            'Authorization': 'Bearer ' + self.bearerToken,
            'Content-Type': 'application/x-www-form-urlencoded'
            }
        return requests.get(endpoint, headers=headers).json()

    #UPDATE CLUSTER
    def patchUpdateCluster(self, data):

        print("patchUpdateCluster: {0}".format(data))

        if(time.time() > self.bearerTokenExpiry):
            print('Auth Token expired, getting a new one!')
            self.getBearerToken()
        
        endpoint = "https://" + self.domain + "/public/api/v1/cluster/" + self.clusterID

        headers = {
            'Authorization': 'Bearer ' + self.bearerToken,
            'Content-Type': 'application/json'
            }

        response = requests.patch(endpoint, data = json.dumps(data), headers=headers)
        return response.json()

    #DEF TURN OFF AND TURN ON CLUSTER
    def effectUpdatesOnCluster(self):

        #TURN OFF CLUSTER
        self.patchUpdateCluster({'enabled': False})

        #WAIT a bit
        time.sleep(30)

        #Turn on cluster
        self.patchUpdateCluster({'enabled': True})

        iteration = 0
        maxIterations = 60
        currentClusterState = self.getCluster()['clusterState']
        print(currentClusterState)
        
        #Loop while cluster <> Running
        while((iteration < maxIterations) and currentClusterState != 'RUNNING'):
            time.sleep(10)
            currentClusterState = self.getCluster()['clusterState']
            print(currentClusterState, datetime.datetime.fromtimestamp(time.time()))
            iteration += 1

    def changeClusterSize(self, numberOfWorkers):
  
        #autoscaling
        if(isinstance(numberOfWorkers, tuple)):
            if(numberOfWorkers[0] > numberOfWorkers[1]):
                print("changeClusterSize: {0}, {1}".format(numberOfWorkers[1], numberOfWorkers[0]))
                data = {'minWorkers': numberOfWorkers[1], 'maxWorkers': numberOfWorkers[0]}
            else:
                print("changeClusterSize: {0}, {1}".format(numberOfWorkers[0], numberOfWorkers[1]))
                data = {'minWorkers': numberOfWorkers[0], 'maxWorkers': numberOfWorkers[1]}
        else:
            print("changeClusterSize: {0}".format(numberOfWorkers))
            data = {'minWorkers': numberOfWorkers, 'maxWorkers': numberOfWorkers}

        self.patchUpdateCluster(data)
        #self.effectUpdatesOnCluster()

    def changeClusterType(self, clusterTypeToTest):

        print("changeClusterType: {0}".format(clusterTypeToTest))

        match clusterTypeToTest:
            case "standard":
                data = {'processingMode' : None}

            case "batch":
                data = {'processingMode' : 'BATCH', 'idleStopMinutes' : 60}

            case "ws":
                data = {'processingMode' : 'WARP_SPEED', 'idleStopMinutes' : 60}
        print(data)
        self.patchUpdateCluster(data)
        #self.effectUpdatesOnCluster()

    def changeResultCaching(self, resultCaching):

        print("changeResultCaching: {0}".format(resultCaching))

        if(resultCaching) == 0:
            data = {'resultCacheEnabled' : False, 'resultCacheDefaultVisibilitySeconds' : None}
        else:
            data = {'resultCacheEnabled' : True, 'resultCacheDefaultVisibilitySeconds' : resultCaching}
        print(data)
        self.patchUpdateCluster(data)
        #self.effectUpdatesOnCluster()

    def __init__(self, clientID, key, domain, clusterName) -> None:

        clusterURL = (domain.split('.', 1))

        galaxyCredentials_string = clientID + ":" + key
        galaxyCredentials_encoded = galaxyCredentials_string.encode("ascii")
        galaxyCredentials_base64_bytes = base64.b64encode(galaxyCredentials_encoded)
        galaxyCredentials_base64_string = galaxyCredentials_base64_bytes.decode("ascii")

        self.auth = galaxyCredentials_base64_string
        self.domain = domain

        self.bearerToken = None
        self.bearerTokenExpiry = time.time()

        self.clusterName = clusterURL[0] + "-" + clusterName + ".trino." + clusterURL[1]
        self.allClusters = self.listCluster()
        self.clusterID = self.setClusterID()
        
if __name__ == "__main__":
    galaxyUser = "galaxyUser/role"
    galaxyPassword = "galaxyPassword"

    galaxyPort = '443'

    galaxyCluster = 'galaxyCluster'
    galaxyURL = 'domain.galaxy.starburst.io'

    galaxyAPIClientID = "galaxyAPIClientID"
    galaxyAPIKey = "galaxyAPIKey"


    clusterSizesToTest = [2, 4, 8]
    RunsPerQuery = 5
    clusterTypesToTest = ['standard', 'batch', 'ws']
    clusterAutoScalingToTest = [(1,2), (1,3)]

    resultCaching = [0]

    _domain = GalaxyDomain(galaxyAPIClientID, galaxyAPIKey, galaxyURL, galaxyCluster)
    _domain.getCluster()

    galaxyHost = galaxyURL.split('.', 1)[0] + "-" + galaxyCluster + ".trino." + galaxyURL.split('.', 1)[1]

    trino_conn = trino.dbapi.connect(
                host=galaxyHost,
                port=galaxyPort,
                user=galaxyUser,
                http_scheme='https',
                auth=trino.auth.BasicAuthentication(galaxyUser, galaxyPassword.encode("utf-8"))
    )

    resultsDF = pd.DataFrame()
    listOfConfigs = []

    executionStartTime = datetime.datetime.now(pytz.timezone('UTC'))

    source_dir = Path('')

    ##Main Loop
    for clusterType in clusterTypesToTest:

        _domain.changeClusterType(clusterType)

        for clusterSize in clusterSizesToTest:
            _domain.changeClusterSize(clusterSize)

            for resultCache in resultCaching:
                #Cast to integer as non-integer cache value not supported
                resultCache = int(resultCache)
                _domain.changeResultCaching(resultCache)
                _domain.effectUpdatesOnCluster()
                
                files = source_dir.iterdir()
                files = source_dir.glob('*.sql')
                
                for file in files:
                    sql = open(file, mode='r', encoding='utf-8-sig').read().replace(';', '')

                    for runPerQuery in range(RunsPerQuery):
                        print("run number: {0}".format(runPerQuery))
                        print("fixedClusterSize, file: {0}, runNumber: {1}, min: {2}, max: {3}, clusterType: {4}, resultCaching: {5}".format(file, runPerQuery, clusterSize, clusterSize, clusterType, resultCache))
                        
                        queryData = Query(sql)
                        print(queryData[0])

                        #listOfConfigs.append((file, runPerQuery, clusterType, clusterSize, clusterSize, resultCache, queryData[0][0],queryData[0][1], str(queryData[0][12]-queryData[0][10]), queryData[0][13], queryData[0][14]))
                        listOfConfigs.append((file, runPerQuery, clusterType, clusterSize, clusterSize, resultCache, queryData[0][0]))

        for clusterAutoScale in clusterAutoScalingToTest:
            #we don't support autoscaling for WS type clusters right now (October 10, 2023)
            if(((clusterType) == 'ws') and (clusterAutoScale[0] != clusterAutoScale[1])):
                print("Autoscaling on Warp Speed not currently supported! Skipping test...")
                continue

            _domain.changeClusterSize(clusterAutoScale)

            for resultCache in resultCaching:
                #Cast to integer as non-integer cache value not supported
                resultCache = int(resultCache)
                _domain.changeResultCaching(resultCache)
                _domain.effectUpdatesOnCluster()
                
                files = source_dir.iterdir()
                files = source_dir.glob('*.sql')
                
                for file in files:
                    sql = open(file, mode='r', encoding='utf-8-sig').read().replace(';', '')

                    for runPerQuery in range(RunsPerQuery):
                        #print("run number: {0}".format(runPerQuery))
                        print("autoScaleCluster, file: {0}, runNumber: {1}, min: {2}, max: {3}, clusterType: {4}, resultCaching: {5}".format(file, runPerQuery, str(clusterAutoScale[0]), str(clusterAutoScale[1]), clusterType, resultCache))
                        
                        queryData = Query(sql)

                        print(queryData[0])
                        #listOfConfigs.append((file, runPerQuery, clusterType, clusterAutoScale[0], clusterAutoScale[1], resultCache, queryData[0][0],queryData[0][1], str(queryData[0][12]-queryData[0][10]), queryData[0][13], queryData[0][14]))
                        listOfConfigs.append((file, runPerQuery, clusterType, clusterAutoScale[0], clusterAutoScale[1], resultCache, queryData[0][0]))

    
    #print(listOfConfigs)
    executionStopTime = datetime.datetime.now(pytz.timezone('UTC'))
    currentTime = datetime.datetime.now(pytz.timezone('UTC'))

    #resultsDF = pd.DataFrame(listOfConfigs, columns=['file','runPerQuery','clusterType','minSize','maxSize','resultCache','query_id','query_state', 'runtime', 'error_type', 'error_code'])
    resultsDF = pd.DataFrame(listOfConfigs, columns=['file','runPerQuery','clusterType','minSize','maxSize','resultCache','query_id'])
    resultsDF.to_csv('results_' + str(currentTime) + '_.csv')



    #Change Cluster back to a moderate size, as we just need to pull Telemetry

    #Take a time snapshot
    currentTime = time.time()
    print(currentTime)

    print("\n\nModerating Cluster Size...\n")

    _domain.changeClusterSize(1)
    _domain.changeClusterType('standard')
    _domain.changeResultCaching(0)
    _domain.effectUpdatesOnCluster()

    print("\n\nSleeping for 1 hour to allow Telemetry data to catch up...\n")
    while(time.time() - currentTime < 3600):
        time.sleep(30)
        print(time.time() - currentTime)

    # timer = 0
    # while timer < 60:
    #     print("Sleeping for: " + str(3600 - timer * 60) + " seconds")
    #     time.sleep(60)
    #     timer += 1


    query = "SELECT query_id, query_state, round(to_unixtime(end_time) - to_unixtime(execution_start_time),3), query_state, index_and_cache_usage_overall, index_and_cache_usage_filtering, index_and_cache_usage_projection FROM \"galaxy_telemetry\".\"public\".\"query_history\" where create_time BETWEEN TIMESTAMP '" + str(executionStartTime).split("+")[0] + "' AND TIMESTAMP '" + str(executionStopTime + datetime.timedelta(seconds=60)).split("+")[0] + "'"
    telemetryDF = pd.DataFrame(issueQuery(query, True), columns=['query_id', 'query_state', 'runtime', 'query_state', 'index_and_cache_usage_overall', 'index_and_cache_usage_filtering', 'index_and_cache_usage_projection'])
    resultsDF.merge(telemetryDF, on='query_id', how='left').to_csv('results_with_telemetry' + str(currentTime) + '_.csv')