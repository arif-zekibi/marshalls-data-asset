# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b914bd3d-f8f9-40d9-a814-9c2c4db325a7",
# META       "default_lakehouse_name": "lh_bronze_landing",
# META       "default_lakehouse_workspace_id": "0570d2f2-4789-4601-91b2-caceb33c9ce2",
# META       "known_lakehouses": [
# META         {
# META           "id": "c2bc7757-3294-426a-a235-3ea7db1e1aea"
# META         },
# META         {
# META           "id": "938e5954-d7a9-4061-9819-43d6ae2f3fb9"
# META         },
# META         {
# META           "id": "b914bd3d-f8f9-40d9-a814-9c2c4db325a7"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# PARAMETERS CELL ********************

TaskID ="-295905668"
Lineage_id = "development"
ETLLastModifiedTimeStamp = "2025-03-14T09:03:58.0110053"


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

try:
    #Get the metadata based on job parameet
    strMetadataQuery = "SELECT DISTINCT concat(c.host,'/',so.folder_path,database_name,'_',so.schema_name,'_',so.object_name) as filedirectory FROM dbo.log l WITH(NOLOCK) INNER JOIN dbo.task t ON l.task_id = t.task_id INNER JOIN dbo.object so ON so.object_id = t.sourceobjectid INNER JOIN dbo.job j ON j.job_id = t.job_id INNER JOIN dbo.connection c ON c.connection_id = so.connection_id WHERE 1=1 AND t.isactive=1 AND t.task_id = "+TaskID
    DfTaskDetails = executeSelectQuery(strMetadataQuery)
    display(DfTaskDetails)
    print(strMetadataQuery)
except Exception as e:
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget-Sub",process_parameter=""\
        ,process_details="Error while running metadata query"\
        ,starttime=StartTime,endtime="NULL",status='failed',error_log=str(e),dq_log="NULL",source_record_count="NULL")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    # Initialize variables.
    
    dfRow = DfTaskDetails.collect()[0]
    
    strfiledirectory = dfRow['filedirectory']
    strFilePath = strfiledirectory + '/' + ETLLastModifiedTimeStamp +'.parquet'
    
except Exception as e:
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadParquetWithModifiedDate-Sub",process_parameter=""\
        ,process_details="Error while initializing variables."\
        ,starttime=StartTime,endtime="NULL",status='failed',error_log=str(e),dq_log="NULL",source_record_count=source_record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getDirandFileName(strFilePath):
    try:
        details = os.path.split(strFilePath)
        return details[0],details[1]
    except Exception as e:
        raise Exception(e,"Error in function getDirandFileName().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def generateParquetWithTimeStampColumn(orgFilePath):
    try:
        if notebookutils.fs.exists(orgFilePath):
            orgFileDir,orgFileName = getDirandFileName(orgFilePath)
            newFileDir = orgFileDir+'/temp'
            df = spark.read.load(path=orgFilePath,format='parquet')
            df = df.withColumn('etllastmodifiedtimestamp',f.lit(ETLLastModifiedTimeStamp).cast('timestamp'))
            df.repartition(1).write.mode('overwrite').parquet(path = newFileDir)
            # print(df.columns)
            newFiles = notebookutils.fs.ls(newFileDir)
            for file in newFiles:
                # print(file.path)
                # print(os.path.splitext(file.path)[1])
                if os.path.splitext(file.path)[1] == '.parquet':
                    print('Filemove in progress..!!')
                    notebookutils.fs.rm(orgFilePath)
                    notebookutils.fs.mv(file.path,orgFileDir+'/NEW_'+orgFileName)
                    notebookutils.fs.rm(newFileDir,True)
                    # notebookutils.fs.cp(file.path,orgFilePath)
    except Exception as e:
        raise Exception(e,"Error in function generateParquetWithTimeStampColumn().")

    # notebookutils.fs.ls('abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/') 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    generateParquetWithTimeStampColumn(strFilePath)
except Exception as e:
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadParquetWithModifiedDate-Sub",process_parameter=""\
        ,process_details="Error while calling generateParquetWithTimeStampColumn()."\
        ,starttime=StartTime,endtime="NULL",status='failed',error_log=str(e),dq_log="NULL",source_record_count=source_record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
