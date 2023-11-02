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

#Issue Query to Galaxy, either returning the result, or just the stats depending on boolean flag resultsRequested
def issueQuery(query, resultsRequested, trino_conn):
    # open the cursor
    rows = None

    cur = trino_conn.cursor()
    #try executing the query
    try:
        cur.execute(query)
        rows = cur.fetchall()
        print("query: {0}, stats: {1}".format(query, cur.stats))

    except Exception as e:
        print(e)
    
    if(resultsRequested):
        cur.close()
        print("Returning Results")
        return rows
    
    stats = cur.stats
    rows = None
    cur.close()

    return stats

#Issue Actual Query
def Query(query, trino_conn):
    getStats = issueQuery(query, False, trino_conn)
    #results = issueQuery('SELECT * FROM system.runtime.queries where source = \'trino-python-client\' and query_id = ' + "'" + getStats['queryId'] + "'", True, trino_conn)
    results = issueQuery('SELECT query_id FROM system.runtime.queries where source = \'trino-python-client\' and query_id = ' + "'" + getStats['queryId'] + "'", True, trino_conn)

    return results

class GalaxyDomain:
    
    #get a Bearer Token
    def getBearerToken(self):

        auth_endpoint = "https://" + self.domain + "/oauth/v2/token"

        headers = {
            'Authorization': 'Basic ' + self.auth,
            'Content-Type': 'application/x-www-form-urlencoded'
            }
        data = {'grant_type': 'client_credentials'}

        response = requests.post(auth_endpoint, data=data, headers=headers)

        try:
            self.bearerToken, self.bearerTokenExpiry = response.json()['access_token'], time.time() + response.json()['expires_in']
        except Exception as e:
            print(e)
            print("Error getting bearerToken - check your credentials")
            self.bearerToken, self.bearerTokenExpiry = None, None
            
    #Find the Cluster by name
    def findCluster(self, clusterName):

        for currentCluster in self.allClusters:
            if "trinoUri" in currentCluster.keys():
                if currentCluster['trinoUri'] == 'https://' + clusterName + ':443':
                    print(f"clusterName: {clusterName}, minWorkers: {currentCluster['minWorkers']}, maxWorkers: {currentCluster['maxWorkers']}, batchMode: {currentCluster['batchCluster']}, warpSpeedCluster: {currentCluster['warpResiliencyEnabled']}")
                    finalCluster = currentCluster

        return finalCluster

    #Set the ClusterID of the Cluster
    def setClusterID(self, clusterName):
        
        clusterDetails = self.findCluster(clusterName)

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
    def getCluster(self, clusterID):

        if(time.time() > self.bearerTokenExpiry):
            print('Auth Token expired, getting a new one!')
            self.getBearerToken()

        endpoint = "https://" + self.domain + "/public/api/v1/cluster/" + clusterID
        
        headers = {
            'Authorization': 'Bearer ' + self.bearerToken,
            'Content-Type': 'application/x-www-form-urlencoded'
            }
        
        return requests.get(endpoint, headers=headers).json()

    #Update Cluster
    def patchUpdateCluster(self, data, clusterID):

        print("patchUpdateCluster: {0}, clusterID: {1}".format(data, clusterID))

        if(time.time() > self.bearerTokenExpiry):
            print('Auth Token expired, getting a new one!')
            self.getBearerToken()
        
        endpoint = "https://" + self.domain + "/public/api/v1/cluster/" + clusterID

        headers = {
            'Authorization': 'Bearer ' + self.bearerToken,
            'Content-Type': 'application/json'
            }

        response = requests.patch(endpoint, data = json.dumps(data), headers=headers)
        
        return response.json()

    #Disable and then Re-enable Cluster to apply Updates
    def effectUpdatesOnCluster(self, clusterID):

        #Disable Cluster
        self.patchUpdateCluster({'enabled': False}, clusterID)

        #Wait
        time.sleep(30)

        #Enable Cluster
        self.patchUpdateCluster({'enabled': True}, clusterID)

        iteration = 0
        maxIterations = 20
        currentClusterState = self.getCluster(clusterID)['clusterState']
        print(currentClusterState)
        
        #Loop while cluster <> Running
        while((iteration < maxIterations) and currentClusterState != 'RUNNING'):
            time.sleep(30)
            currentClusterState = self.getCluster(clusterID)['clusterState']
            print(currentClusterState + ", clusterId: " + clusterID + ", @", datetime.datetime.fromtimestamp(time.time()))
            iteration += 1
    
    #Change the size of the cluster (including autoscaling)
    def changeClusterSize(self, numberOfWorkers, clusterID):
  
        #Check if autoscaling
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

        self.patchUpdateCluster(data, clusterID)

    #Change the cluster Type
    def changeClusterType(self, clusterTypeToTest, clusterID):

        print("changeClusterType: {0}".format(clusterTypeToTest))

        match clusterTypeToTest:
            case "standard":
                data = {'processingMode' : None}

            case "batch":
                data = {'processingMode' : 'BATCH', 'idleStopMinutes' : 60}

            case "ws":
                data = {'processingMode' : 'WARP_SPEED', 'idleStopMinutes' : 60}
        print(data)
        self.patchUpdateCluster(data, clusterID)

    #Change QueryResultCaching
    def changeResultCaching(self, resultCaching, clusterID):

        print("changeResultCaching: {0}".format(resultCaching))

        if(resultCaching) == 0:
            data = {'resultCacheEnabled' : False, 'resultCacheDefaultVisibilitySeconds' : None}
        else:
            data = {'resultCacheEnabled' : True, 'resultCacheDefaultVisibilitySeconds' : resultCaching}
        print(data)
        self.patchUpdateCluster(data, clusterID)


    #Change a Cluster Status
    def changeClusterStatus(self, enable, clusterID):

        if(enable) == 0:
            print("Disable Cluster: {0}".format(clusterID))
            data = {'enabled' : False}
        else:
            print("Enable Cluster: {0}".format(clusterID))
            data = {'enabled' : True}

        print(data)

        self.patchUpdateCluster(data, clusterID)

    #Find a specific catalog
    def findCatalog(self, catalogName):

        finalCatalog = None
        
        for currentCatalog in self.allCatalogs['result']:
            if(currentCatalog['catalogName'] == catalogName):
               # print(f"catalogName: {currentCatalog['catalogName']}, catalogId: {currentCatalog['catalogId']}")
                finalCatalog = currentCatalog

        return finalCatalog

    #Set the CatalogID based on catalog name
    def setCatalogID(self, catalogName):
        
        catalogDetails = self.findCatalog(catalogName)

        return catalogDetails['catalogId']

    #Find all catalogs in a domain
    def listCatalog(self):
        if(time.time() > self.bearerTokenExpiry):
            print('Auth Token expired, getting a new one!')
            self.getBearerToken()

        endpoint = "https://" + self.domain + "/public/api/v1/catalog"
        
        headers = {
            'Authorization': 'Bearer ' + self.bearerToken,
            'Content-Type': 'application/x-www-form-urlencoded'
            }
        
        return requests.get(endpoint, headers=headers).json()

    #Determine the cluster type, based on supplied clusterID
    def getClusterType(self, clusterID):
        clusterStatus = self.getCluster(clusterID)

        clusterType = None

        if(clusterStatus['batchCluster'] == True):
            clusterType = "batch"
        elif(clusterStatus['warpSpeedCluster'] == True):
            clusterType = "ws"
        else:
            clusterType = "standard"
            
        return clusterType
    
    #Create a new cluster
    def createCluster(self, data):

        if(time.time() > self.bearerTokenExpiry):
            print('Auth Token expired, getting a new one!')
            self.getBearerToken()

        endpoint = "https://" + self.domain + "/public/api/v1/cluster"

        headers = {
            'Authorization': 'Bearer ' + self.bearerToken,
            'Content-Type': 'application/json'
            }

        response = requests.post(endpoint, data = json.dumps(data), headers=headers)

        return response.json()['clusterId']

    #Delete a cluster
    def deleteCluster(self, clusterId):

        if(time.time() > self.bearerTokenExpiry):
            print('Auth Token expired, getting a new one!')
            self.getBearerToken()

        endpoint = "https://" + self.domain + "/public/api/v1/cluster/" + clusterId

        headers = {
            'Authorization': 'Bearer ' + self.bearerToken,
            'Content-Type': 'application/json'
            }

        requests.delete(endpoint, headers=headers)

    #Initialize
    def __init__(self, clientID, key, domain, testClusterName) -> None:

        clusterURL = (domain.split('.', 1))

        galaxyCredentials_string = clientID + ":" + key
        galaxyCredentials_encoded = galaxyCredentials_string.encode("ascii")
        galaxyCredentials_base64_bytes = base64.b64encode(galaxyCredentials_encoded)
        galaxyCredentials_base64_string = galaxyCredentials_base64_bytes.decode("ascii")

        self.auth = galaxyCredentials_base64_string
        self.domain = domain

        self.bearerToken = None
        self.bearerTokenExpiry = time.time()

        self.testClusterName = clusterURL[0] + "-" + testClusterName + ".trino." + clusterURL[1]
        
        self.allClusters = self.listCluster()
        self.testClusterID = self.setClusterID(self.testClusterName)

        self.allCatalogs = self.listCatalog()
        self.telemetryCatalogID = self.setCatalogID('galaxy_telemetry')

if __name__ == "__main__":
    
    #Define the credentials used to issue queries to Galaxy
    galaxyUser = "galaxyUser/role"
    galaxyPassword = "galaxyPassword"

    #Port to Galaxy (default is 443)
    galaxyPort = '443'

    #Define the Galaxy URL used for testing
    galaxyURL = 'domain.galaxy.starburst.io'

    #Define the Galaxy Cluster used for testing
    galaxyTestCluster = 'galaxyCluster'

    #Galaxy API Credentials to interact with APIs
    galaxyAPIClientID = "galaxyAPIClientID"
    galaxyAPIKey = "galaxyAPIKey"

    #Define where your SQL files containing your queries are stored (default is in same directory as this script)
    source_dir = Path('SQLfiles')

    ##Enter the parameters you wish to test here. 
    #clusterSizesToTest accepts a tuple
    #runsPerQuery only accepts a single value
    #clusterTypesToTest and resultCaching must have at least one value
    clusterSizesToTest = [(1,1), (2,2), (1,2), (2,3)]
    runsPerQuery = 3
    clusterTypesToTest = ['standard', 'batch', 'ws']
    resultCaching = [0, 300, 600]

    _domain = GalaxyDomain(galaxyAPIClientID, galaxyAPIKey, galaxyURL, galaxyTestCluster)

    trino_conn_testing = trino.dbapi.connect(
                host=galaxyURL.split('.', 1)[0] + "-" + galaxyTestCluster + ".trino." + galaxyURL.split('.', 1)[1],
                port=galaxyPort,
                user=galaxyUser,
                http_scheme='https',
                auth=trino.auth.BasicAuthentication(galaxyUser, galaxyPassword.encode("utf-8"))
    )

    resultsDF = pd.DataFrame()
    listOfConfigs = []

    executionStartTime = datetime.datetime.now(pytz.timezone('UTC'))

    for clusterType in clusterTypesToTest:

        _domain.changeClusterType(clusterType, _domain.testClusterID)

        for clusterSize in clusterSizesToTest:
            
            #We don't support autoscaling for WS type clusters right now (October 10, 2023), skip the test
            if(((clusterType) == 'ws') and (clusterSize[0] != clusterSize[1])):
                print("Autoscaling on Warp Speed not currently supported! Skipping test...")
                listOfConfigs.append((None, None, clusterType, clusterSize[0], clusterSize[1], resultCache, "Autoscaling on Warp Speed not currently supported! Skipping test..."))
                continue

            #We don't recommend autoscaling for FTE type clusters right now (October 25, 2023), skip the test
            if(((clusterType) == 'batch') and (clusterSize[0] != clusterSize[1])):
                print("Autoscaling on FTE not recommended! Skipping test...")
                listOfConfigs.append((None, None, clusterType, clusterSize[0], clusterSize[1], resultCache, "Autoscaling on FTE not recommended! Skipping test..."))
                continue

            _domain.changeClusterSize(clusterSize, _domain.testClusterID)

            for resultCache in resultCaching:
                #Cast to integer as non-integer cache value not supported
                resultCache = int(resultCache)

                if((resultCache < 300) and (resultCache != 0)):
                    print("Minimum Cache Reuse Period setting is 300 seconds, resetting your value of: {0} to 300s".format(resultCache))
                    resultCache = 300

                _domain.changeResultCaching(resultCache, _domain.testClusterID)
                _domain.effectUpdatesOnCluster(_domain.testClusterID)
                
                if(_domain.getClusterType(_domain.testClusterID) != 'ws' and clusterType == 'ws'):
                    print("Unable to switch to Warp Speed cluster, accelerated clusters are only available for upto 1 data lake catalog. Please make sure you only have 1 data lake catalog connected")
                    listOfConfigs.append((None, None, clusterType, clusterSize[0], clusterSize[1], resultCache, "Unable to switch to Warp Speed cluster, accelerated clusters are only available for upto 1 data lake catalog. Please make sure you only have 1 data lake catalog connected"))
                    continue

                files = source_dir.iterdir()
                files = source_dir.glob('*.sql')
                
                for file in files:
                    sql = open(file, mode='r', encoding='utf-8-sig').read().replace(';', '')

                    for runPerQuery in range(runsPerQuery):
                        print("run number: {0}".format(runPerQuery))
                        print("File: {0}, runNumber: {1}, min: {2}, max: {3}, clusterType: {4}, resultCaching: {5}".format(file, runPerQuery, clusterSize[0], clusterSize[1], clusterType, resultCache))
                        
                        listOfConfigs.append((file, runPerQuery, clusterType, clusterSize[0], clusterSize[1], resultCache, issueQuery(sql, False, trino_conn_testing)['queryId']))
    
    #print(listOfConfigs)
    executionStopTime = datetime.datetime.now(pytz.timezone('UTC'))

    #Disable the test cluster
    _domain.changeClusterStatus(0, _domain.testClusterID)

    #Save the parameters to a DF/csv
    resultsDF = pd.DataFrame(listOfConfigs, columns=['file','runPerQuery','clusterType','minSize','maxSize','resultCache','query_id'])
    resultsDF.to_csv('results_' + str(executionStopTime) + '_.csv')

    #Take a time snapshot
    currentTime = time.time()

    #Sleep for 1 hour, let telemetry catch up
    print("\nSleeping for 1 hour to allow Telemetry data to populate...\n")
    while(time.time() - currentTime < 3600):
        time.sleep(120)
        print("Minutes left: {0}".format(round((3600 - (time.time() - currentTime))/60,1)))

    #Create the telemetry cluster. It must be created in AWS US-EAST-1, as that is the cloud/region of Galaxy Telemetry Catalog
    data = {
    'name' : _domain.telemetryCatalogID,
    'cloudRegionId' : 'aws-us-east1',
    'catalogRefs' : [
        _domain.telemetryCatalogID
    ],
    'idleStopMinutes' : 5,
    'minWorkers' : 1,
    'maxWorkers' : 1,
    'warpResiliencyEnabled' : False,
    'resultCacheEnabled' : False
    }

    telemetryClusterId = _domain.createCluster(data)
    _domain.effectUpdatesOnCluster(telemetryClusterId)

    #connection to the telemetry cluster
    trino_conn_telemetry = trino.dbapi.connect(
                host=galaxyURL.split('.', 1)[0] + "-" + _domain.telemetryCatalogID + ".trino." + galaxyURL.split('.', 1)[1],
                port=galaxyPort,
                user=galaxyUser,
                http_scheme='https',
                auth=trino.auth.BasicAuthentication(galaxyUser, galaxyPassword.encode("utf-8"))
    )

    #Get the telemetry data, and join it to the parameters data
    #Finally save the complete results as a csv
    query = "SELECT query_id, round(to_unixtime(end_time) - to_unixtime(execution_start_time),3), query_state, index_and_cache_usage_overall, index_and_cache_usage_filtering, index_and_cache_usage_projection FROM \"galaxy_telemetry\".\"public\".\"query_history\" where create_time BETWEEN TIMESTAMP '" + str(executionStartTime).split("+")[0] + "' AND TIMESTAMP '" + str(executionStopTime + datetime.timedelta(seconds=60)).split("+")[0] + "'"
    telemetryDF = pd.DataFrame(issueQuery(query, True, trino_conn_telemetry), columns=['query_id', 'runtime', 'query_state', 'index_and_cache_usage_overall', 'index_and_cache_usage_filtering', 'index_and_cache_usage_projection'])
    resultsDF.merge(telemetryDF, on='query_id', how='left').to_csv('results_with_telemetry' + str(executionStopTime) + '_.csv')

    #Delete the telemetry cluster
    _domain.deleteCluster(telemetryClusterId)
