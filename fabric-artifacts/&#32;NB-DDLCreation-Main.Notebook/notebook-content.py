# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
JobName = 'silverlhtosqlserver'
DatabaseType = 'sqlserverdb'
Lineage_id = "development"

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

#Get the task_id for the job
strMetadataQuery = "SELECT t.task_id FROM dbo.task t  INNER JOIN dbo.job j ON j.job_id = t.job_id \
INNER JOIN dbo.object so ON so.object_id = t.sourceobjectid \
INNER JOIN dbo.object do ON do.object_id = t.targetobjectid \
WHERE t.isactive=1  AND do.isactive =1 AND do.iscreated = 0 AND j.name='"+JobName+"'"
print(strMetadataQuery)
dfMetadata = executeSelectQuery(strMetadataQuery)
# display(dfMetadata)

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

# CELL ********************

def executeNotebook(TaskID):
    try:
        args={}
        args['TaskID'] = str(TaskID)
        args['DatabaseType']= 'sqlserverdb'
        args['Lineage_id'] = Lineage_id
        notebookutils.notebook.run(path=" NB-DDLCreation-Sub" ,timeout_seconds=90,arguments=args)
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Select TaskId for Iterative Load

import notebookutils.mssparkutils
import concurrent.futures
if dfMetadata != None:
    listTask = dfMetadata.select('task_id').rdd.flatMap(lambda x: x).collect()
    with concurrent.futures.ThreadPoolExecutor(max_workers = 5) as executor:
        future = [executor.submit(executeNotebook,TaskID) for TaskID in listTask]    
    

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
