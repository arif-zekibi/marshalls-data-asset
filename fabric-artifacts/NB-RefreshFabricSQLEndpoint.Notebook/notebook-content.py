# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

LakehouseName = 'dataverse_mrshd365cete_cds2_workspace_unqf9fb2400291e4fce8525104f6a65a'
Lineage_id = 'development'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time
import struct
import sqlalchemy
import pyodbc
import notebookutils
import pandas as pd
from pyspark.sql import functions as fn
from datetime import timedelta
import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run Utility

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

processParamter= {}
lakehousename = LakehouseName
processParamter['lakehousename'] =lakehousename
processParamter = str(processParamter).replace("'","")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# !pip install semantic-link --q 


def pad_or_truncate_string(input_string, length, pad_char=' '):
    # Truncate if the string is longer than the specified length
    if len(input_string) > length:
        return input_string[:length]
    # Pad if the string is shorter than the specified length
    return input_string.ljust(length, pad_char)

## not needed, but usefull
tenant_id=spark.conf.get("trident.tenant.id")
workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")
lakehouse_name=LakehouseName

#sql_endpoint= fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']

#Instantiate the client
client = fabric.FabricRestClient()
# print(fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json())
workspacedetails = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()
# print(workspacedetails['value'])
for item in workspacedetails['value']:
    if item['displayName'] == lakehousename:
        # print(item)
        sqlEndPointConnectionString = item['properties']['sqlEndpointProperties']['connectionString']
        sqlEndPointId = item['properties']['sqlEndpointProperties']['id']
        # print(sqlEndPointConnectionString,sqlEndPointId)

# URI for the call
uri = f"/v1.0/myorg/lhdatamarts/{sqlEndPointId}"
# This is the action, we want to take
payload = {"commands":[{"$type":"MetadataRefreshExternalCommand"}]}

# Call the REST API
response = client.post(uri,json= payload)
## You should add some error handling here

# return the response from json into an object we can get values from
data = json.loads(response.text)
print(data)

# We just need this, we pass this to call to check the status
batchId = data["batchId"]

# the state of the sync i.e. inProgress
progressState = data["progressState"]

# URL so we can get the status of the sync
statusuri = f"/v1.0/myorg/lhdatamarts/{sqlEndPointId}/batches/{batchId}"

statusresponsedata = ""
RefreshStartTime = datetime.datetime.now()
while progressState == 'inProgress' :
    # For the demo, I have removed the 1 second sleep.
    time.sleep(10)

    # check to see if its sync'ed
    #statusresponse = client.get(statusuri)

    # turn response into object
    statusresponsedata = client.get(statusuri).json()
    # print(statusresponsedata)
    # get the status of the check
    progressState = statusresponsedata["progressState"]
    # show the status
    display(f"Sync state: {progressState}")
    RefreshEndTime = datetime.datetime.now()
    RefreshTime = RefreshEndTime - RefreshStartTime
    if RefreshTime.seconds > 240:
        progressState =  'failure' 
        break

# if its good, then create a temp results, with just the info we care about
if progressState == 'success':
    table_details = [
        {
        'tableName': table['tableName'],
         'warningMessages': table.get('warningMessages', []),
         'lastSuccessfulUpdate': table.get('lastSuccessfulUpdate', 'N/A'),
         'tableSyncState':  table['tableSyncState'],
         'sqlSyncState':  table['sqlSyncState']
        }
        for table in statusresponsedata['operationInformation'][0]['progressDetail']['tablesSyncStatus'] if table['sqlSyncState'] != 'NotRun'
    ]
    tableDetails = json.dumps(table_details, indent=2)
    

# if its good, then shows the tables
if progressState == 'success':
    # # Print the extracted details
    # print("Extracted Table Details:")
    # for detail in table_details:
    #     print(f"Table: {pad_or_truncate_string(detail['tableName'],30)}   Last Update: {detail['lastSuccessfulUpdate']}  tableSyncState: {detail['tableSyncState']}   Warnings: {detail['warningMessages']}")
    LogError(taskid="NULL",Lineage_id=Lineage_id,processed_by="NB-RefreshFabricSQLEndpoint",process_parameter=processParamter\
        ,process_details=""\
        ,starttime=RefreshStartTime.strftime('%Y-%m-%d %H:%M:%S'),endtime=RefreshEndTime.strftime('%Y-%m-%d %H:%M:%S'),status='1'\
        ,error_log=tableDetails,dq_log="NULL",source_record_count="NULL")


## if there is a problem, show all the errors
if progressState == 'failure':
    # display error if there is an error
    LogError(taskid="NULL",Lineage_id=Lineage_id,processed_by="NB-RefreshFabricSQLEndpoint",process_parameter=processParamter\
        ,process_details=json.dumps(statusresponsedata,indent=2)\
        ,starttime=RefreshStartTime.strftime('%Y-%m-%d %H:%M:%S'),endtime=RefreshEndTime.strftime('%Y-%m-%d %H:%M:%S'),status='0',\
        error_log="Lakehouse sql endpoint refresh is Failed.",dq_log="NULL",source_record_count="NULL")

    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
