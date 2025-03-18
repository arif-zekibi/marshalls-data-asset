# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "938e5954-d7a9-4061-9819-43d6ae2f3fb9",
# META       "default_lakehouse_name": "lh_silver_correction",
# META       "default_lakehouse_workspace_id": "0570d2f2-4789-4601-91b2-caceb33c9ce2"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
JobName = 'silverlhtosqlserver'
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
# strMetadataQuery = "SELECT t.task_id FROM dbo.task t INNER JOIN dbo.job j ON j.job_id = t.job_id WHERE t.isactive=1 AND j.name='"+JobName+"'"
strMetadataQuery = "SELECT task_id FROM dbo.vwtask WHERE jobname='"+JobName+"'"
dfMetadata = executeSelectQuery(strMetadataQuery)
# display(dfMetadata)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

listTask = dfMetadata.select('task_id').rdd.flatMap(lambda x: x).collect()
print(listTask)

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
        args['Lineage_id'] = Lineage_id
        args['useRootDefaultLakehouse']= True
        if JobName == 'silverlhtosqlserver':
            notebookutils.notebook.run(path="NB-LoadDataSourceToTarget-Sub-SQLDB" ,timeout_seconds=90,arguments=args)
        else:
            notebookutils.notebook.run(path="NB-LoadDataSourceToTarget-Sub" ,timeout_seconds=90,arguments=args)
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
with concurrent.futures.ThreadPoolExecutor(max_workers = 5) as executor:
    future = [executor.submit(executeNotebook,TaskID) for TaskID in listTask]    
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
