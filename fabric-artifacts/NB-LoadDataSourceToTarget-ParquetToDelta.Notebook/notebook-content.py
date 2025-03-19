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

ETLLastModifiedTimeStamp,ValidFromDate,ValidToDate,StartTime,ViewTimeStamp =  generateTimeStamp()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    #Get the metadata based on job parameet
    strMetadataQuery = "SELECT "\
        "t.task_id "\
        ",CASE "\
        "WHEN so.database_name LIKE '%dataverse%' THEN CONCAT(so.database_name,'.'+so.schema_name,'.'+so.object_name) "\
        "ELSE LOWER(CONCAT(so.database_name,'.'+so.schema_name,'_'+so.object_name)) END "\
        "as SourceTableName "\
        ",CASE WHEN so.database_name LIKE '%lh%' THEN lower(so.object_name) ELSE so.object_name END as SoTableName"\
        ",CASE WHEN so.database_name LIKE '%lh%' THEN lower(so.database_name) ELSE so.database_name END as SourceDatabaseName"\
        ",COALESCE(CASE WHEN so.database_name LIKE '%lh%' THEN lower(so.schema_name) ELSE so.schema_name end,'') as SourceSchemaName "\
        ",CONCAT('/Tables/',CASE WHEN so.database_name LIKE '%lh%' THEN LOWER(CONCAT(so.schema_name+'/',so.object_name)) ELSE CONCAT(so.schema_name+'/',so.object_name) END) as SourceTablePath "\
        ",LOWER(CONCAT(do.database_name,'.'+do.schema_name,'.'+do.object_name)) as TargetTableName"\
        ",CONCAT('/Tables/',LOWER(CONCAT(do.schema_name,'/',do.object_name))) as TargetTablePath"\
        ",LOWER(COALESCE(do.object_name,'')) as DoTableName"\
        ",LOWER(COALESCE(do.database_name,'')) as TargetDatabaseName"\
        ",LOWER(COALESCE(do.schema_name,'')) as TargetSchemaName "\
        ",COALESCE(CASE WHEN so.database_name LIKE '%lh%' THEN LOWER(tc.source_column_name) ELSE tc.source_column_name END,'') as SourceColumn "\
        ",COALESCE(tc.source_column_dtype,'') as SourceColumnDataType "\
        ",COALESCE(so_con.host,'') as SourceURL "\
        ",COALESCE(do_con.host,'') as TargetURL "\
        ",COALESCE(so.folder_path,'') as folder_path "\
        ",LOWER(COALESCE(tc.target_column_name,'')) as TargetColumn "\
        ",COALESCE(do_con.user_name_secret,'') as TargetUserName,COALESCE(do_con.password_secret,'') as TargetPassword"\
        ",CASE  "\
        "WHEN do_con.connection_type='lakehouse' THEN "\
        "CASE "\
        "WHEN tc.target_column_dtype = 'real' THEN 'float'  "\
        "WHEN tc.target_column_dtype = 'int' THEN 'INT'  "\
        "WHEN tc.target_column_dtype in ('nvarchar','varchar','uniqueidentifier') THEN 'STRING'  "\
        "WHEN tc.target_column_dtype in ('decimal','float') THEN tc.target_column_dtype   "\
        "WHEN tc.target_column_dtype in ('bigint') THEN 'BIGINT'  "\
        "WHEN tc.target_column_dtype IN ('datetime2') THEN 'TIMESTAMP'  "\
        "WHEN tc.target_column_dtype IN ('bit') THEN 'TINYINT'  "\
        "ELSE tc.target_column_dtype  "\
        "END "\
        "WHEN do_con.connection_type in ('sqlserver','azuresqldatabase') THEN "\
        "CASE "\
        "WHEN tc.target_column_dtype = 'real' THEN 'FLOAT'  "\
        "WHEN tc.target_column_dtype = 'int' THEN 'INT'  "\
        "WHEN tc.target_column_dtype in ('nvarchar','varchar','uniqueidentifier') THEN 'VARCHAR(MAX)' "\
        "WHEN tc.target_column_dtype in ('decimal','float') THEN tc.target_column_dtype  "\
        "WHEN tc.target_column_dtype in ('bigint') THEN 'BIGINT'  "\
        "WHEN tc.target_column_dtype IN ('datetime2','datetime','timestamp') THEN 'DATETIME2'  "\
        "WHEN tc.target_column_dtype  in ('date') THEN 'DATE' "\
        "WHEN tc.target_column_dtype IN ('bit') THEN 'TINYINT'  "\
        "ELSE tc.target_column_dtype  "\
        "END "\
        "END as TargetColumnDataType  "\
        ",COALESCE(t.is_incremental,0) as is_incremental "\
        ",COALESCE(CASE WHEN so.database_name LIKE '%lh%' THEN LOWER(t.incremental_column) ELSE t.incremental_column END,'') as incremental_column "\
        ",COALESCE(CASE  WHEN t.incremental_column_dtype = 'int' THEN 'INT' WHEN t.incremental_column_dtype in ('nvarchar','varchar','uniqueidentifier') "\
        "THEN 'STRING' WHEN t.incremental_column_dtype in ('decimal','float') THEN 'DECIMAL(38,6)' WHEN t.incremental_column_dtype in ('bigint') "\
        "THEN 'BIGINT' WHEN t.incremental_column_dtype IN ('datetime2','timestamp') THEN 'TIMESTAMP' WHEN t.incremental_column_dtype IN ('bit') "\
        "THEN 'TINYINT' ELSE t.incremental_column_dtype END,'') as incremental_column_dtype "\
        ",COALESCE(t.incremental_operator,'') incremental_operator "\
        ",COALESCE(t.incremental_value,'') as incremental_value "\
        ",CAST(t.is_merge as VARCHAR(1)) AS is_merge "\
        ",CAST(tc.is_primary as VARCHAR(1)) AS is_primary "\
        ",CAST(tc.is_scd1 as VARCHAR(1)) AS is_scd1 "\
        ",CAST(tc.is_scd2 as VARCHAR(1)) AS is_scd2"\
        ",COALESCE(CASE WHEN so.database_name LIKE '%lh%' THEN LOWER(t.partition_orderby_column) ELSE t.partition_orderby_column END,'etllastmodifiedtimestamp') as partition_orderby_column"\
        ",COALESCE(do.iscreated,1) iscreated"\
        ",so.object_id as SourceObjectID"\
        ",do.object_id as TargetObjectID "\
        "FROM dbo.task t INNER JOIN dbo.taskcolumnmapping tc ON t.task_id = tc.task_id INNER JOIN dbo.object so  ON so.object_id = t.sourceobjectid "\
        "INNER JOIN dbo.object do ON do.object_id = t.targetobjectid INNER JOIN dbo.job j ON j.job_id = t.job_id "\
        "INNER JOIN dbo.connection so_con ON so_con.connection_id = so.connection_id "\
        "INNER JOIN dbo.connection do_con on do_con.connection_id=do.connection_id "\
        "WHERE tc.isactive=1 and t.isactive=1 "\
        "AND so.isactive=1 AND do.isactive=1 and j.isactive=1 AND t.task_id="+TaskID
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

#Prepare Column Mapping for data load
def getColumnMapping():
    try:
        listColumnMapping = DfTaskDetails.select(['SourceColumn','SourceColumnDataType','TargetColumn','TargetColumnDataType','is_primary','is_scd1','is_scd2']).toPandas().to_dict(orient='records')
        return listColumnMapping
    except Exception as e:
        raise Exception(e,"Error in getColumnMapping().")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    # Initialize variables.
    
    dfRow = DfTaskDetails.collect()[0]
    listOfMapping =  getColumnMapping()
    strWriteMode = 'incremental' if dfRow['is_incremental'] == 1 else 'full'
    SCD2Cols = [l['TargetColumn'] for l in listOfMapping if l['is_scd2']=='1']
    strUpdateType=""
    if len(SCD2Cols) > 0:
        strUpdateType = 'scd2'
    strSourceTable = dfRow['SourceTableName']
    # strTargetTable = dfRow['TargetTableName']
    strTargetTable = dfRow['TargetDatabaseName']+'.'+dfRow['DoTableName']
    strIncrementalColumn = dfRow['incremental_column']
    strIncrementalColDataType = dfRow['incremental_column_dtype']
    strIncremantalColOperator = dfRow['incremental_operator']
    strIncrementalColValue = dfRow['incremental_value']
    strInsertType = 'merge' if dfRow['is_merge'] == 1 else 'insert'
    strTaskID = str(dfRow['task_id'])
    strPartitionOrderByColumn = str(dfRow['partition_orderby_column'])
    lstCheckSumColums = ['scd1checksum','scd2checksum']
    lstIDColumns = ['dwh_id']
    lstAuditColumns = ['etllastmodifiedtimestamp']
    lstSCD2Cols = ['validfromdate','validtodate','validflag']
    strTargetSchemanName = dfRow['TargetSchemaName']
    strTargetDatabaseName = dfRow['TargetDatabaseName']
    strTargetTableSubPath = dfRow['TargetTablePath']
    strSourceTableSubPath = dfRow['SourceTablePath']
    strSourceDatabaseName = dfRow['SourceDatabaseName']
    strSourceSchemaName = dfRow['SourceSchemaName']
    strSourceURL = dfRow['SourceURL']
    strTargetURL = dfRow['TargetURL']
    strSoTableName = dfRow['SoTableName']
    strDoTableName = dfRow['DoTableName']
    strTableForSchema = strTargetDatabaseName+'.'+strTargetSchemanName+'.'+'TableForSchema'
    ## strTempTableName = strTargetDatabaseName+'.'+strTargetSchemanName+'.'+'temp_'+strDoTableName.lower()
    strTempTableName = strTargetDatabaseName+'.'+'temp_'+strDoTableName.lower()
    strTargetTableForSchemaPath = strTargetURL+'/Tables/'+strTargetSchemanName+'/TableForSchema'
    strTargetTablePath = strTargetDatabaseName+'.'+strDoTableName.lower()
    ##strSourceTablePath = strSourceURL+'/'+strSourceTableSubPath
    folder_path = dfRow['folder_path']
    strSourceTablePath = strSourceURL+folder_path+strSourceDatabaseName+'_'+strSourceSchemaName+'_'+strSoTableName
    strTempSourceView = 'temp_'+strSoTableName.lower()
    strTempViewName = 'vw_'+strSourceTable.lower().replace('.','_')+'_'+ViewTimeStamp
    strTargetTableCreated = str(dfRow['iscreated'])
    strTargetTableCreated = 1 ##########str(dfRow['iscreated'])

    strSourceTableId = str(dfRow['SourceObjectID'])
    strTargetTableId = str(dfRow['TargetObjectID'])
    strISMerge = dfRow['is_merge']
    strDQMessage = "NULL"
    source_record_count = "NULL"
    print(ViewTimeStamp,strTempViewName,strWriteMode,strUpdateType,strSourceTable,strTargetTable,strIncrementalColumn,strIncrementalColDataType,strIncremantalColOperator,strIncrementalColValue,strInsertType\
    ,strSourceDatabaseName,strSourceSchemaName,strSourceTablePath,strTargetDatabaseName,strTargetSchemanName,strTargetTablePath,strSourceURL,strTargetURL
    ,strSoTableName,strDoTableName,strTempTableName,strSourceTableId,strTargetTableId)
except Exception as e:
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget-ParquetToDelta",process_parameter=""\
        ,process_details="Error while initializing variables."\
        ,starttime=StartTime,endtime="NULL",status='failed',error_log=str(e),dq_log="NULL",source_record_count=source_record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    #Generating process processParamter,processMsg
    processParamter= {}
    processParamter['ColumnMapping'] =listOfMapping
    processParamter['SourceTablePath'] =""
    processParamter['TargetTablePath'] =""
    processParamter['SourceTableName'] =strSourceTable
    processParamter['TargetTableName'] =strTargetTable
    processParamter['WriteMode'] =strWriteMode
    processParamter['UpdateType'] =strUpdateType
    processParamter['IncrementalColumn'] = strIncrementalColumn
    processParamter['IncrementalColDataType'] = strIncrementalColDataType
    processParamter['IncrementalOperator'] = strIncremantalColOperator
    processParamter['IncrematalColValue'] = strIncrementalColValue
    processParamter = str(processParamter).replace("'","")
    processMsg = 'Loading data from '+strSourceTable+' to '+strTargetTable+'.'
    # print(processParamter)
except Exception as e:
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget",process_parameter=""\
        ,process_details="Error while initializing processParamter."\
        ,starttime=StartTime,endtime="NULL",status='failed',error_log=str(e),dq_log="NULL",source_record_count=source_record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import ast
from delta.tables import *
from pyspark.sql.functions import col,max,lit,xxhash64
import traceback


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
# Create a SparkSession object
spark = SparkSession.builder \
    .appName("DataIngestionApp") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getPrimaryKey(tableType ='target'):
    try:
        # Prepare Merge Condition
        listPrimaryKey = ['target.' + l['TargetColumn']+' =  source.' +l['SourceColumn'] for l in listOfMapping if l['is_primary']=='1']
        strPrimaryKey = ' AND '.join(listPrimaryKey)
        if tableType == 'target':
            listPrimaryKey = [l['TargetColumn'] for l in listOfMapping if l['is_primary']=='1']
        elif tableType == 'source':
            listPrimaryKey = [l['SourceColumn'] for l in listOfMapping if l['is_primary']=='1']
        return strPrimaryKey,listPrimaryKey
        # print(strPrimaryKey)
    except Exception as e:
        raise Exception(e,"Error in function getPrimaryKey().")

def getSCDCheckSumColumns(tableType):
    try:
        #Start Arif 20241031
        #'xxhash64(`UPRN`,`validfromdate`) as dwh_id'
        strPrimaryKey,lstPrimaryKey = getPrimaryKey(tableType)
        tableType = 'SourceColumn' if tableType =='source' else 'TargetColumn' 
        SCD1CheckSum = [col[tableType] for col in listOfMapping if col['is_primary'] =='0' and col['is_scd1']=='1' and col[tableType] != '`etllastmodifiedtimestamp`']
        SCD2CheckSum = [col[tableType] for col in listOfMapping if col['is_primary'] =='0' and col['is_scd2']=='1' and col[tableType] != '`etllastmodifiedtimestamp`']
        SCD1CheckSum.sort()
        SCD2CheckSum.sort()
        strSCD1CheckSum ='xxhash64('+",".join(SCD1CheckSum)+') as scd1checksum' if len(SCD1CheckSum)>0 else "xxhash64('') as scd1checksum"
        strSCD2CheckSum='xxhash64('+",".join(SCD2CheckSum)+') as scd2checksum' if len(SCD2CheckSum)>0 else "xxhash64('') as scd2checksum"
        lstColForDuplicateCheck = []
        if len(lstPrimaryKey) > 0:
            lstColForDuplicateCheck = lstPrimaryKey
        else:
            lstColForDuplicateCheck = [l['SourceColumn'] for l in listOfMapping]
        lstColForDuplicateCheck.sort()
        strPrimaryColCheckSum='xxhash64('+",".join(lstColForDuplicateCheck)+')' if len(lstColForDuplicateCheck)>0 else "xxhash64('')"
        return strSCD1CheckSum,strSCD2CheckSum,strPrimaryColCheckSum
        #End Arif 20241031
    except Exception as e:
        raise Exception(e,"Error in function getSCDCheckSumColumns().")
def getColANDPrimaryKey(tableType='target'):
    try:
        colMaping = []
        _,listPrimarKey = getPrimaryKey(tableType)
        for m in listOfMapping:
            if m['SourceColumn'] != '`etllastmodifiedtimestamp`':
                colMaping.append((m['SourceColumn']+' AS '+m['TargetColumn']))
            else:
                colMaping.append("to_timestamp('"+ETLLastModifiedTimeStamp+"') AS etllastmodifiedtimestamp")
        if strUpdateType == 'scd2':
            SCDCols = ["to_timestamp('1900-01-01') AS validfromdate","to_timestamp('2099-12-31 23:59:59') AS validtodate","1 as validflag"]
            colMaping.extend(SCDCols)
            listPrimarKey.append('`validfromdate`')
        # Add Surrogate Key
        strSurrogateKey = 'xxhash64('+",".join(listPrimarKey)+') as dwh_id'
        #Start Arif 20241031
        strSCD1CheckSum,strSCD2CheckSum,_ = getSCDCheckSumColumns(tableType)
        colMaping.append(strSCD1CheckSum)
        colMaping.append(strSCD2CheckSum)
        #End Arif 20241031
        colMaping.append(strSurrogateKey)
        return colMaping,listPrimarKey
    except Exception as e:
        raise Exception(e,"Error in function getColANDPrimaryKey().")
# print(colMaping)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Get List of SCD2 Columns
def getMatchedCondition(colsType):
    strMatchedCondition = ""
    try:
        if colsType == 'scd1':
            strMatchedCondition = 'source.scd1checksum <> target.scd1checksum'
        elif colsType == 'scd2':
            strMatchedCondition = 'source.scd2checksum <> target.scd2checksum'
        return strMatchedCondition
    #End Arif 20241031
    except Exception as e:
        raise Exception(e,"Error in function getMatchedCondition().")

def getInsertCols():
    try:
        strInsertCols = "INSERT ({0}) VALUES ({1})"
        _,listPrimarKey = getColANDPrimaryKey(tableType='source')
        targetCols = []
        sourceCols =[]
        hashKey =""
        for col in listOfMapping:
            targetCols.append(col['TargetColumn'])
            sourceCols.append('source.'+col['SourceColumn'])
            # print(targetCols)
        #Start Arif 20241031
        targetCols.append('etllastmodifiedtimestamp')
        sourceCols.append('to_timestamp(\''+ETLLastModifiedTimeStamp+'\')')
        targetCols.extend(lstCheckSumColums)
        
        sourceCols.extend(lstCheckSumColums)
        #End Arif 20241031
        hashKey = ',xxhash64(source.'+",source.".join(listPrimarKey)+')'
        hashKey = hashKey.replace('source.`validfromdate`',"to_timestamp('"+ValidFromDate+"')")
        # SCD2Cols = [l['TargetColumnName'] for l in listOfMapping if l['is_scd2']=='1']
        if len(SCD2Cols) > 0 :
            SCDColsNames = ['validfromdate','validtodate','validflag']
            SCDColsValues = ["to_timestamp('1900-01-01')","to_timestamp('2099-12-31 23:59:59')","1"]
            targetCols.extend(SCDColsNames)
            sourceCols.extend(SCDColsValues)
        targetCols.extend(lstIDColumns)
        strInsertCols = strInsertCols.format(",".join(targetCols),",".join(sourceCols)+hashKey)
        #Add Surrogate Key Column
        
        # print(strInsertCols)
        return strInsertCols
    except Exception as e:
        raise Exception(e,"Error in function getInsertCols().")
        
def getUpdateCols(colsType):
    try:
        if colsType == 'scd1':
            colsType = 'is_scd1'
            strCheckSumColumns = 'target.scd1checksum = source.scd1checksum'
        else:
            colsType = 'is_scd2'
            strCheckSumColumns = 'target.scd2checksum = source.scd2checksum'
        strUpdateCols = "UPDATE SET {0} "
        updateCols = []
        for col in listOfMapping:
            # if col['scd2'] == colsType and col['is_primary'] =='0':
            if col[colsType] == '1' and col['is_primary'] =='0':
                updateCols.append('target.'+col['TargetColumn'] +' = '+'source.'+col['SourceColumn'])
            # print(targetCols)
        updateCols.append('target.`etllastmodifiedtimestamp`' +' = '+'to_timestamp(\''+ETLLastModifiedTimeStamp+'\')')
        updateCols.append(strCheckSumColumns)
        strUpdateCols = strUpdateCols.format(",".join(updateCols))
        return strUpdateCols
    except Exception as e:
        raise Exception(e,"Error in function getUpdateCols().")

def getColumnsFromMapping (colsType,tableType,AppendString,ReplaceETLModfiledDate):
    cols = []
    isAllColumnNeeded=0
    try:
        if colsType == 'scd1':
            colsType = 'is_scd1'
        elif colsType == 'scd2':
            colsType = 'is_scd2'
        else: 
            isAllColumnNeeded = 1
            colsType = 'is_scd2'

        for col in listOfMapping:
            if (col[colsType] == '1' or isAllColumnNeeded == 1) :
                if tableType == 'source':
                    cols.append (col['SourceColumn'])
                elif tableType == 'target':
                    cols.append (col['TargetColumn'])
        cols.extend(lstCheckSumColums)
        return AppendString.join(cols)
    except Exception as e:
        raise Exception(e,"Error in function getColumnsFromMapping().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getMergeQuery():
    try:
        strQuery = 'MERGE into {TargetTable} as target USING {SourceTable} as source ON {PrimaryKey} '+\
            'WHEN MATCHED AND {MatchedCondition} THEN {UpdateCols}' +\
            'WHEN NOT MATCHED THEN {InsertCols}'
        
        strPrimaryKey,_ = getPrimaryKey()
        strMatchedCondition = getMatchedCondition('scd1') 
        if strMatchedCondition == "()":
            strMatchedCondition = '1=2'
        strUpdateCols = getUpdateCols('scd1')
        strInsertCols = getInsertCols()
        strQuery = strQuery.format(SourceTable = strTempViewName,TargetTable = strTargetTable,PrimaryKey = strPrimaryKey,\
            MatchedCondition = strMatchedCondition,UpdateCols=strUpdateCols,InsertCols = strInsertCols)
        # print(strQuery)
        return strQuery
    except Exception as e:
        raise Exception(e,"Error in function getMergeQuery().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Get Insert Query
def getInsertQuery():
    try:
        strInsertQuery = "INSERT INTO {TargetTable} ({TargetCols}) SELECT {SourceCols} FROM {SourceTable} as source"
        strTargetColumns = getColumnsFromMapping('ALL','target',",",ReplaceETLModfiledDate=False)
        strTargetColumns = strTargetColumns + ',etllastmodifiedtimestamp,dwh_id'
        strSourceColumns = 'source.'+getColumnsFromMapping('ALL','source',",source.",ReplaceETLModfiledDate=False)
        _,listPrimarKey = getColANDPrimaryKey(tableType='source')
        hashKey = ',xxhash64(source.'+",source.".join(listPrimarKey)+') as dwh_id'
        strSourceColumns = strSourceColumns +',to_timestamp(\''+ETLLastModifiedTimeStamp+'\')'+ hashKey
        strInsertQuery = strInsertQuery.format(TargetTable = strTargetTable ,TargetCols =strTargetColumns\
                                    ,SourceCols =strSourceColumns,SourceTable = strTempViewName)
        # print(strSourceColumns)
        # print(strInsertQuery)
        return strInsertQuery
    except Exception as e:
        raise Exception(e,"Error in function getMergeQuery().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Opertion for SCD2 
def ProcessSCD2Data():
    try:
        # SCD2Cols = [l for l in listOfMapping if l['scd2']=='1']
        # strUpdateValidDateQuery = "UPDATE "+strTargetTable+" SET validtodate='2024-01-01' WHERE validflag=1"
        strInsertNewSCD2RecordsQuery = "INSERT INTO {TargetTable} ({TargetCols}) SELECT {SourceCols} FROM {SourceTable} as source"+\
                                    " INNER JOIN {TargetTable} as target ON {PrimaryKey} "+\
                                    " WHERE target.validflag = 1 AND {MatchCondition} "
        strTargetColumns = getColumnsFromMapping('ALL','target',",",ReplaceETLModfiledDate=False)
        strTargetColumns = strTargetColumns + ',etllastmodifiedtimestamp,validfromdate,validtodate,validflag,dwh_id'
        strSourceColumns = 'source.'+getColumnsFromMapping('ALL','source',",source.",ReplaceETLModfiledDate=False)
        strSourceColumns =  strSourceColumns+',to_timestamp(\''+ETLLastModifiedTimeStamp+'\')'
        
        #Prepare Hash Key
        _,listPrimarKey = getColANDPrimaryKey(tableType='source')
        hashKey = ',xxhash64(source.'+",source.".join(listPrimarKey)+') as dwh_id'
        hashKey = hashKey.replace('source.`validfromdate`',"to_timestamp('"+ValidFromDate+"')")
        strSourceColumns = strSourceColumns + ",to_timestamp('"+ValidFromDate+"') AS validfromdate,to_timestamp('2099-12-31 23:59:59') as validtodate, 1 as validflag"\
                            +hashKey
        strPrimaryKey,_ = getPrimaryKey()

        strInsertNewSCD2RecordsQuery = strInsertNewSCD2RecordsQuery.format(TargetTable = strTargetTable ,TargetCols =strTargetColumns\
                                    ,SourceCols =strSourceColumns,SourceTable = strTempViewName,PrimaryKey = strPrimaryKey\
                                    ,MatchCondition = getMatchedCondition('scd2'))
        # strUpdateValidFlagQuery = "UPDATE "+strTargetTable+" SET validflag=0,validtodate=to_timestamp('"+validtodate+"') WHERE validfromdate < '"+validfromdate+"'"
        strUpdateValidFlagQuery = 'MERGE into {TargetTable} as target USING {SourceTable} as source ON {PrimaryKey} '+\
            'WHEN MATCHED AND target.validflag =1 AND target.validfromdate < \''+ValidFromDate+'\' AND {MatchedCondition} THEN UPDATE SET target.validflag =0 '+\
            ',target.validtodate=to_timestamp(\''+ValidToDate+'\'),target.etllastmodifiedtimestamp = to_timestamp(\''+ETLLastModifiedTimeStamp+'\')' 
        strMatchedCondition = getMatchedCondition('scd2')
        strUpdateValidFlagQuery = strUpdateValidFlagQuery.format(TargetTable = strTargetTable,SourceTable = strTempViewName,PrimaryKey = strPrimaryKey,\
            MatchedCondition=strMatchedCondition)
        print('\n........ Start: Insert Script for New SCD2 Records ........\n')
        print(strInsertNewSCD2RecordsQuery,'\n')
        spark.sql(strInsertNewSCD2RecordsQuery)
        print('\n........ End: Insert Script for New SCD2 Records ........\n')
        print('\n........ Start: Update Flag Script to Update Validity Columns ........\n')
        print(strUpdateValidFlagQuery)
        spark.sql(strUpdateValidFlagQuery)
        print('\n........ End: Update Flag Script to Update Validity Columns ........\n')
    except Exception as e:
        raise Exception(e,"Error in function ProcessSCD2Data().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create Table
def createTable():
    try:
        # if not spark.catalog.tableExists(strTargetTable):
        if strTargetTableCreated == '0':
            print('\n........ Start:Create Target Table('+strTargetTable+')........\n')
            data =[{'Column1':'data'}]
            df = spark.createDataFrame(data)
            df.write.mode('append').format('delta')\
                .save(strTargetTableForSchemaPath)
            strCreateTableQuery ='CREATE OR REPLACE TABLE {TableName} ({Columns})'
            listColumns = [l['TargetColumn']+' '+l['TargetColumnDataType'] for l in listOfMapping]
            listCheckSumColWithDt = [l+' ' +'BIGINT' for l in lstCheckSumColums]
            listIDColWithDt = [l+' ' +'BIGINT' for l in lstIDColumns]
            listAuditColsWithDt = [l+' ' +'TIMESTAMP' for l in lstAuditColumns]
            # SCD2Cols = [l['TargetColumnName'] for l in listOfMapping if l['is_scd2']=='1']
            listColumns.extend(listCheckSumColWithDt)
            listColumns.extend(listIDColWithDt)
            listColumns.extend(listAuditColsWithDt)
            if len(SCD2Cols) > 0:
                listColumns.append('validfromdate TIMESTAMP')
                listColumns.append('validtodate TIMESTAMP')
                listColumns.append('validflag TINYINT')
            strTableCols = ' ,'.join(listColumns)
            strCreateTableQuery = strCreateTableQuery.format(TableName=strTargetTable,Columns = strTableCols)
            print(strCreateTableQuery)
            spark.sql(strCreateTableQuery)
            updateObjectCreatedStatus(strTargetTableId,'1')
            print('\n........ End:Create Target Table('+strTargetTable+')........\n')
        else:
            print('\n........Table('+strTargetTable+') is already created........\n')
    except Exception as e:
        raise Exception(e,"Error in function createTable().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def createIncrementalView():
    try:
        print('\n........ Start:Create Incremental Source view........\n')
        df = spark.read.option("mergeSchema","true").parquet(strSourceTablePath)
        df.createOrReplaceTempView(strTempSourceView)
        strCreateTempViewQuery ='CREATE OR REPLACE TEMP VIEW {TempViewName} AS ( SELECT * FROM ( SELECT {SelectCols} {PartitionBy} FROM {SourceTableName} WHERE 1=1 {FilterCondition}) {FilterOnSeq})'
        listColumns = ['`'+l['SourceColumn']+'`'+' AS `'+l['TargetColumn']+'`' for l in listOfMapping]
        listCheckSumColWithDt = [l+' ' +'INT' for l in lstCheckSumColums]
        listAuditColsWithDt = [l+' ' +'TIMESTAMP' for l in lstAuditColumns]
        strSCD1CheckSum,strSCD2CheckSum,strPrimaryColCheckSum = getSCDCheckSumColumns('source')
        strTableCols = ','.join(listColumns)
        strTableCols = strTableCols+','+strSCD1CheckSum+','+strSCD2CheckSum+','+strPrimaryColCheckSum+' as PrimaryColCheckSum,to_timestamp(\''+ETLLastModifiedTimeStamp+'\') AS `etllastmodifiedtimestamp`'
        if len(SCD2Cols) > 0:
            strTableCols= strTableCols + ",to_timestamp('"+ValidFromDate+"') AS validfromdate,to_timestamp('2099-12-31 23:59:59') as validtodate, 1 as validflag"
        strFilterCondition = ""
        strFilterCondition = "AND " +strIncrementalColumn +" "+strIncremantalColOperator+" CAST('"+strIncrementalColValue+"' AS "+strIncrementalColDataType+")"
        strPartitionBy = ",ROW_NUMBER() OVER (PARTITION BY "+strPrimaryColCheckSum+ " ORDER BY `"+strPartitionOrderByColumn+"` DESC) AS Seq"
        strFilterOnSeq = "WHERE Seq=1 "
        strCreateTempViewQuery = strCreateTempViewQuery.format(TempViewName = strTempViewName,SelectCols = strTableCols,PartitionBy = strPartitionBy\
            ,SourceTableName = strTempSourceView, FilterCondition= strFilterCondition,FilterOnSeq = strFilterOnSeq)
        print(strCreateTempViewQuery)
        spark.sql(strCreateTempViewQuery)
        print('\n........ End:Create Incremental Source view........\n')
        
    except Exception as e:
        raise Exception(e,"Error in function createIncrementalView().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Data Quality Check
def dqCheck():
    try:
        _,listPrimaryKey = getPrimaryKey('target')
        # print(listPrimaryKey)
        if len(listPrimaryKey) > 0 :
            strConditionColumn = "xxhash64("+",".join(listPrimaryKey)+")"
            strSelectColumn = strConditionColumn+ " AS HashKey"    
        else:
            strConditionColumn = "xxhash64("+getColumnsFromMapping('ALL','target',",",ReplaceETLModfiledDate=False)+")"
            strSelectColumn = strConditionColumn+ " AS HashKey"
        #check of duplicate
        # For now it's not needed due to check while loading data.
        # strDuplicateCheck = "SELECT {Columns},COUNT(1) Records FROM {TableName} GROUP BY {GroupByColumn} HAVING COUNT(1) > 1"
        # strDuplicateCheck = strDuplicateCheck.format(Columns=strColumn,TableName = strTargetTable,GroupByColumn = strGroupBy)
        # print(strDuplicateCheck)
        #Check for null value in either primary key or all the records.
        # print(strNullCheck)
        strDeleteQuery ="DELETE FROM {TableName} WHERE {ConditionColumns} == 42".format(TableName = strTargetTable,ConditionColumns = strConditionColumn)
        print(strDeleteQuery)
        df = spark.sql(strDeleteQuery)
        return str(df.collect()[0][0])+" Records deleted due to all columns having NULL data."
    except Exception as e:
        raise Exception(e,"Error in function dqCheck().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    #Create Target Table
    # Logic to load data for full load type and create table first time for incremental load type.
    if strWriteMode == 'full':
        print('In Fullload')
        print('\n........ Start: Dropping target table for full load.........\n')
        strTruncateTargetTable = "DROP TABLE IF EXISTS "+strTargetTable
        print(strTruncateTargetTable)
        spark.sql(strTruncateTargetTable)
        strTargetTableCreated='0'
        print('\n........ End: Dropping target table for full load.........\n')
    createTable()
    #Create Incremental view for source table.
    createIncrementalView()
    # check if source is hacing data. if source does not have data not need to proceed further. 
    numOfRecordsInSource = spark.sql("SELECT COUNT(1) Records FROM "+strTempViewName).collect()[0][0]
    print('No of records in Source: ',numOfRecordsInSource)
    if numOfRecordsInSource > 0:
        if strISMerge == '0':
            print('\n........ Start: Inserting data in Target Table.........\n')
            strInsertQuery = getInsertQuery()
            print(strInsertQuery)
            spark.sql(strInsertQuery)
            print('\n........ End: Inserting data in Target Table.........\n')
        elif strISMerge == '1':
            strMergeQuery = getMergeQuery()
            print('\n........ Start: Merge Script to Upsert Data ........\n')
            print(strMergeQuery,'\n')
            spark.sql(strMergeQuery)
            print('\n........ End: Merge Script to Upsert Data ........\n')
            # SCD2Cols = [l['TargetColumnName'] for l in listOfMapping if l['is_scd2']=='1']
            if len(SCD2Cols) > 0:
                print('\n........ Start: Loading SCD2 Data load ........\n')
                ProcessSCD2Data()   
                print('\n........ End: Loading SCD2 Data load ........\n')
        strDQMessage = dqCheck()
    else:
        processMsg = processMsg +'\n No new data from source.'
    updateIncrementalValue(strTaskID,StartTime)
    _,_,_,endTime,_ = generateTimeStamp()
    LogError(taskid=strTaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget",process_parameter=processParamter\
        ,process_details=processMsg\
        ,starttime=StartTime,endtime=endTime,status='success',error_log="NULL",dq_log=strDQMessage,source_record_count = str(numOfRecordsInSource))
except Exception as e:
    if len(e.args)>1:
        processMsg =processMsg+e.args[1]
    else:
        processMsg =processMsg+"Issue in main logic."
    msg = str(traceback.format_exc()).replace('"','`').replace("'",'`')
    # print(msg)
    _,_,_,endTime,_ = generateTimeStamp()
    LogError(taskid=strTaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget",process_parameter=processParamter\
        ,process_details=processMsg\
        ,starttime=StartTime,endtime=endTime,status='failed',error_log=msg,dq_log="NULL",source_record_count=source_record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
