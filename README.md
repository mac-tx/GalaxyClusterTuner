# GalaxyClusterTuner
A Python Script to automate finding the optimal Cluster Configuration in Starburst Galaxy for your workloads.

Getting Ready to Execute the Scripts:
Use an account with the ACCOUNTADMIN role to work through the steps in the guide here - it will help you get set up quickly:

1. Create the catalogs containing the data you will be testing in your queries

2. The tool needs a cluster to work on. Either use an existing cluster, or create a new cluster. Make sure that no other account/application will be using this cluster, as it will be updated automatically and this may cause some downtime for other users if they are using it

3. Connect up the catalogs you created in Step 1 to the cluster you created in Step 2

4. Next, also connect up telemetry to the same cluster you are using for your testing in Step 2. This will help the Python script track query completion times later.

5. Then, create a User or Service Account with the correct role permission to execute queries on the new Catalog/Cluster you created earlier. Follow along here, or, if you wish you can use your current user/role assuming you have the correct permissions to execute queries on the catalogs/schemas/tables/locations you wish to test.

6. Create a Service API (with the ACCOUNTADMIN Role). We will use this Service API to issue cluster changes via the API.

7. Download the code from this GitHub repository. 

8. Install Python Requirements

9. Go to the __main__ section of the code, and update the code with the right authentication parameters to your Galaxy Cluster:
if __name__ == "__main__":
galaxyUser = "galaxyUser/role"
galaxyPassword = "galaxyPassword"
galaxyPort = '443'
galaxyCluster = 'galaxyCluster'
galaxyURL = 'domain.galaxy.starburst.io'
galaxyAPIClientID = "galaxyAPIClientID"
galaxyAPIKey = "galaxyAPIKey"

10. Locate the SQL files you wish to test - keeping a single .SQL file per query you wish to execute. Either keep your SQL files in a subfolder called SQLfiles, or adjust the following path and copy your SQL files there. See the GitHub for example syntax.
source_dir = Path('SQLfiles')

11. Configure the parameters you wish to use in the code
clusterSizesToTest = [(1,1), (2,2), (1,5), (1,7)]
RunsPerQuery = 5
clusterTypesToTest = ['standard', 'batch', 'ws']
resultCaching = [0, 60]

12. Run the code! The code will loop through the configurations you have specified and the queries you wish to test
You will notice the code will pause for ~1 hour. The reason for waiting 1 hour before running the remaining code is that the Telemetry data for a query is made available generally after ~1 hour after the query completes execution. 

13. You should now have your results data (in a csv file) that you can now examine to determine optimal cluster configuration for each query that you tested.
