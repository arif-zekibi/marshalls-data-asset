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
Lineage_id = "development"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    #Get the task_id for the job
    strMetadataQuery = "SELECT t.task_id FROM dbo.task t INNER JOIN dbo.job j ON j.job_id = t.job_id WHERE t.isactive=1 AND j.name='"+JobName+"'"
    dfMetadata = executeSelectQuery(strMetadataQuery)
    # display(dfMetadata)
except Exception as e:
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-DataValidation-Main",process_parameter=""\
        ,process_details="Error while getting task details."\
        ,starttime=StartTime,endtime="NULL",status='failed',error_log=str(e),dq_log="NULL",source_record_count="NULL")

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
