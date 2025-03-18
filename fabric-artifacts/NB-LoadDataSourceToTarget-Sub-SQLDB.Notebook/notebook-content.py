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
# META       "default_lakehouse_workspace_id": "0570d2f2-4789-4601-91b2-caceb33c9ce2",
# META       "known_lakehouses": [
# META         {
# META           "id": "c2bc7757-3294-426a-a235-3ea7db1e1aea"
# META         },
# META         {
# META           "id": "938e5954-d7a9-4061-9819-43d6ae2f3fb9"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# PARAMETERS CELL ********************

TaskID ="-1192108104"
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
        ",COALESCE(CASE WHEN so.database_name LIKE '%lh%' THEN CONCAT('[',LOWER(tc.source_column_name),']') ELSE tc.source_column_name END,'') as SourceColumn "\
        ",COALESCE(tc.source_column_dtype,'') as SourceColumnDataType "\
        ",COALESCE(so_con.host,'') as SourceURL "\
        ",COALESCE(do_con.host,'') as TargetURL "\
        ",CONCAT('[',LOWER(COALESCE(tc.target_column_name,'')),']') as TargetColumn "\
        ",COALESCE(do_con.user_name_secret,'') as TargetUserName,COALESCE(do_con.password_secret,'') as TargetPassword"\
        ",CASE  "\
        "WHEN do_con.connection_type='lakehouse' THEN "\
        "CASE "\
        "WHEN tc.target_column_dtype = 'real' THEN 'float'  "\
        "WHEN tc.target_column_dtype = 'int' THEN 'INT'  "\
        "WHEN tc.target_column_dtype in ('nvarchar','varchar','uniqueidentifier') THEN 'STRING'  "\
        "WHEN tc.target_column_dtype in ('decimal','float') THEN tc.target_column_dtype "\
        "WHEN tc.target_column_dtype in ('bigint') THEN 'BIGINT'  "\
        "WHEN tc.target_column_dtype IN ('datetime2') THEN 'TIMESTAMP'  "\
        "WHEN tc.target_column_dtype IN ('bit') THEN 'TINYINT'  "\
        "ELSE tc.target_column_dtype  "\
        "END "\
        "WHEN do_con.connection_type in ('sqlserver','azuresqldatabase') THEN "\
        "CASE "\
        "WHEN tc.target_column_dtype = 'real' THEN 'FLOAT'  "\
        "WHEN tc.target_column_dtype = 'int' THEN 'INT'  "\
        "WHEN tc.target_column_dtype in ('nvarchar','varchar','uniqueidentifier') THEN tc.target_column_dtype "\
        "WHEN tc.target_column_dtype in ('decimal','float') THEN tc.target_column_dtype "\
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
    print(strMetadataQuery)
    display(DfTaskDetails)
except Exception as e:
    print(strMetadataQuery)
    _,_,_,endTime,_ = generateTimeStamp()
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget-Sub-SQLDB",process_parameter=""\
        ,process_details="Error while running metadata query"\
        ,starttime=StartTime,endtime=endTime,status='failed',error_log=str(e),dq_log="NULL",source_record_count="NULL")

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
    strTargetTable = dfRow['TargetTableName']
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
    strTempTableName = strTargetDatabaseName+'.'+strTargetSchemanName+'.'+'temp_'+strDoTableName.lower()
    strTargetTableForSchemaPath = strTargetURL+'/Tables/'+strTargetSchemanName+'/TableForSchema'
    strTargetTablePath = strTargetURL+'/'+strTargetTableSubPath
    strSourceTablePath = strSourceURL+'/'+strSourceTableSubPath
    strTempSourceView = 'temp_'+strSoTableName.lower()
    strTempViewName = 'vw_'+strSourceTable.lower().replace('.','_')+'_'+ViewTimeStamp
    strTargetTableCreated = str(dfRow['iscreated'])
    strSourceTableId = str(dfRow['SourceObjectID'])
    strTargetTableId = str(dfRow['TargetObjectID'])
    strISMerge = dfRow['is_merge']
    strDQMessage = "NULL"
    source_record_count = "NULL"
    strStgTableNameWithoutSchema = 'stg_'+strDoTableName
    strStgTableNameWithSchema = strTargetTable.replace(strDoTableName,strStgTableNameWithoutSchema)
    strTargetUserName = str(dfRow['TargetUserName'])
    strTargetPassword = str(dfRow['TargetPassword'])
    print(ViewTimeStamp,strTempViewName,strWriteMode,strUpdateType,strSourceTable,strTargetTable,strIncrementalColumn,strIncrementalColDataType,strIncremantalColOperator,strIncrementalColValue,strInsertType\
    ,strSourceDatabaseName,strSourceSchemaName,strSourceTablePath,strTargetDatabaseName,strTargetSchemanName,strTargetTablePath,strSourceURL,strTargetURL
    ,strSoTableName,strDoTableName,strTempTableName,strSourceTableId,strTargetTableId,strStgTableNameWithoutSchema,strStgTableNameWithSchema,strTargetUserName,strTargetPassword)
    # listOfMapping
except Exception as e:
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget-Sub-SQLDB",process_parameter=""\
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
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget-Sub-SQLDB",process_parameter=""\
        ,process_details="Error while initializing processParamter."\
        ,starttime=StartTime,endtime="NULL",status='failed',error_log=str(e),dq_log="NULL",source_record_count=source_record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

configValue = 'true'
if strSourceTable == "lh_silver_correction.d365_bomcalctrans":
    configValue = 'false'  
print(configValue)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import ast
from delta.tables import *
from pyspark.sql.functions import col,max,lit
import traceback
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

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
    .config("spark.sql.execution.arrow.pyspark.enabled",configValue)\
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
        # listPrimaryKey = ['target.' + l['TargetColumn']+' =  source.' +l['TargetColumn'] for l in listOfMapping if l['is_primary']=='1']
        strPrimaryKey = ' AND '.join(listPrimaryKey)
        if tableType == 'target':
            listPrimaryKey = [l['TargetColumn'] for l in listOfMapping if l['is_primary']=='1']
        elif tableType == 'source':
            listPrimaryKey = [l['SourceColumn'] for l in listOfMapping if l['is_primary']=='1']
            # listPrimaryKey = [l['TargetColumn'] for l in listOfMapping if l['is_primary']=='1']
        return strPrimaryKey,listPrimaryKey
        # print(strPrimaryKey)
    except Exception as e:
        raise Exception(e,"Error in function getPrimaryKey().")

def getSCDCheckSumColumns(tableType):
    try:
        #Start Arif 20241031
        #'xxhash64(UPRN,validfromdate) as dwh_id'
        strPrimaryKey,lstPrimaryKey = getPrimaryKey(tableType)
        tableType = 'SourceColumn' if tableType =='source' else 'TargetColumn' 
        # tableType = 'TargetColumn' if tableType =='source' else 'TargetColumn' 
        SCD1CheckSum = [col[tableType] for col in listOfMapping if col['is_primary'] =='0' and col['is_scd1']=='1' and col[tableType] != 'etllastmodifiedtimestamp']
        SCD2CheckSum = [col[tableType] for col in listOfMapping if col['is_primary'] =='0' and col['is_scd2']=='1' and col[tableType] != 'etllastmodifiedtimestamp']
        SCD1CheckSum.sort()
        SCD2CheckSum.sort()
        strSCD1CheckSum ='xxhash64('+",".join(SCD1CheckSum)+') as scd1checksum' if len(SCD1CheckSum)>0 else "xxhash64('') as scd1checksum"
        strSCD2CheckSum='xxhash64('+",".join(SCD2CheckSum)+') as scd2checksum' if len(SCD2CheckSum)>0 else "xxhash64('') as scd2checksum"
        lstColForDuplicateCheck = []
        if len(lstPrimaryKey) > 0:
            lstColForDuplicateCheck = lstPrimaryKey
        else:
            lstColForDuplicateCheck = [l['SourceColumn'] for l in listOfMapping]
            # lstColForDuplicateCheck = [l['TargetColumn'] for l in listOfMapping]
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
            if m['SourceColumn'] != 'etllastmodifiedtimestamp':
                colMaping.append((m['SourceColumn']+' AS '+m['TargetColumn']))
            # if m['TargetColumn'] != 'etllastmodifiedtimestamp':
            #     colMaping.append((m['TargetColumn']+' AS '+m['TargetColumn']))
            else:
                colMaping.append("to_timestamp('"+ETLLastModifiedTimeStamp+"') AS etllastmodifiedtimestamp")
        if strUpdateType == 'scd2':
            SCDCols = ["to_timestamp('1900-01-01') AS validfromdate","to_timestamp('2099-12-31 23:59:59') AS validtodate","1 as validflag"]
            colMaping.extend(SCDCols)
            listPrimarKey.append('validfromdate')
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

def isColumnConfigured(columnName:str):
    try:
        if DfTaskDetails != None:
            records = int(DfTaskDetails.filter(f"TargetColumn = '{columnName}'").count())
            if records != None and records > 0:
                return 1
            else:
                return 0
    except Exception as e:
        raise Exception(e,"Error in function checkColumnIsConfigured().")

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
            # sourceCols.append('source.'+col['TargetColumn'])
            # print(targetCols)
        #Start Arif 20241031
        targetCols.append('etllastmodifiedtimestamp')
        sourceCols.append('cast(\''+ETLLastModifiedTimeStamp+'\' as datetime2)')
        targetCols.extend(lstCheckSumColums)
        
        sourceCols.extend(lstCheckSumColums)
        #End Arif 20241031
        hashKey = ',CHECKSUM(hashbytes(\'md5\',cast(source.'+",cast(source.".join(listPrimarKey)+' as varchar(500))))'
        hashKey = hashKey.replace('source.validfromdate',"cast('"+ValidFromDate+"' as datetime2)")
        # SCD2Cols = [l['TargetColumnName'] for l in listOfMapping if l['is_scd2']=='1']
        if len(SCD2Cols) > 0 :
            SCDColsNames = ['validfromdate','validtodate','validflag']
            # SCDColsValues = ["to_timestamp('1900-01-01')","to_timestamp('2099-12-31 23:59:59')","1"]
            SCDColsValues = ["cast('1900-01-01' as datetime2)","cast('2099-12-31 23:59:59' as datetime2)","1"]
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
                # updateCols.append('target.'+col['TargetColumn'] +' = '+'source.'+col['TargetColumn'])
            # print(targetCols)
        updateCols.append('target.etllastmodifiedtimestamp' +' = '+'cast(\''+ETLLastModifiedTimeStamp+'\' as datetime2)')
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
                    # cols.append (col['TargetColumn'])
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
            'WHEN NOT MATCHED {strNullValueFilter} THEN {InsertCols};'
        strColumnFilter = ''
        if isColumnConfigured('[recid]') > 0:
            strColumnFilter = ' AND source.[recid] is not NULL '
        strPrimaryKey,_ = getPrimaryKey()
        strMatchedCondition = getMatchedCondition('scd1') 
        if strMatchedCondition == "()":
            strMatchedCondition = '1=2'
        strUpdateCols = getUpdateCols('scd1')
        strInsertCols = getInsertCols()
        strQuery = strQuery.format(SourceTable = strStgTableNameWithSchema,TargetTable = strTargetTable,PrimaryKey = strPrimaryKey,\
            MatchedCondition = strMatchedCondition+strColumnFilter,UpdateCols=strUpdateCols,strNullValueFilter = strColumnFilter, InsertCols = strInsertCols)
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
        print(listPrimarKey)
        hashKey = ',xxhash64(source.'+",source.".join(listPrimarKey)+') as dwh_id'
        strSourceColumns = strSourceColumns +',to_timestamp(\''+ETLLastModifiedTimeStamp+'\')'+ hashKey
        strInsertQuery = strInsertQuery.format(TargetTable = strTargetTable ,TargetCols =strTargetColumns\
                                    ,SourceCols =strSourceColumns,SourceTable = strStgTableNameWithSchema)
        # print(strSourceColumns)
        # print(strInsertQuery)
        return strInsertQuery
    except Exception as e:
        raise Exception(e,"Error in function getInsertQuery().")

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
        hashKey = hashKey.replace('source.validfromdate',"to_timestamp('"+ValidFromDate+"')")
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
        # spark.sql(strInsertNewSCD2RecordsQuery)
        print('\n........ End: Insert Script for New SCD2 Records ........\n')
        print('\n........ Start: Update Flag Script to Update Validity Columns ........\n')
        print(strUpdateValidFlagQuery)
        # spark.sql(strUpdateValidFlagQuery)
        print('\n........ End: Update Flag Script to Update Validity Columns ........\n')
    except Exception as e:
        raise Exception(e,"Error in function ProcessSCD2Data().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def createSchema():
    try:
        if strTargetTableCreated == '0':
            print('\n........ Start:Create Target SQL Schema('+strTargetSchemanName+')........\n')
            strCreateSchema = 'CREATE SCHEMA '+strTargetSchemanName
            print(strCreateSchema)
            sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
            executeDMLQueryOnTarget(strCreateSchema,sqlConnection)
            print('\n........ End:Create Target SQL Schema('+strTargetSchemanName+')........\n')
        else:
            print('\n........Schema('+strTargetSchemanName+') is already created........\n')
    except Exception as e:
        print('\n........Schema('+strTargetSchemanName+') is already created........\n')

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
            strCreateTableQueryFormat ="IF NOT EXISTS (SELECT T.NAME,S.NAME FROM SYS.TABLES T INNER JOIN SYS.SCHEMAS S ON T.SCHEMA_ID = S.SCHEMA_ID WHERE T.NAME='{TableNameWithOutSchema}' AND S.NAME='"+strTargetSchemanName+"') BEGIN CREATE TABLE {TableName} ({Columns}) END"
            listColumns = [l['TargetColumn']+' '+l['TargetColumnDataType'] for l in listOfMapping]
            listCheckSumColWithDt = [l+' ' +'BIGINT' for l in lstCheckSumColums]
            listIDColWithDt = [l+' ' +'BIGINT' for l in lstIDColumns]
            listAuditColsWithDt = [l+' ' +'DATETIME2' for l in lstAuditColumns]
            listColumns.extend(listCheckSumColWithDt)
            listColumns.extend(listIDColWithDt)
            listColumns.extend(listAuditColsWithDt)
            if len(SCD2Cols) > 0:
                listColumns.append('validfromdate datetime2')
                listColumns.append('validtodate datetime2')
                listColumns.append('validflag TINYINT')
            strTableCols = ' ,'.join(listColumns)
            strCreateTableQuery = strCreateTableQueryFormat.format(TableNameWithOutSchema = strDoTableName, TableName=strTargetTable,Columns = strTableCols)
            print(strCreateTableQuery)
            strCreateStagingTable = strCreateTableQueryFormat.format(TableNameWithOutSchema = strStgTableNameWithoutSchema,TableName=strStgTableNameWithSchema,Columns = strTableCols)
            print(strCreateStagingTable)
            sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
            executeDMLQueryOnTarget(strCreateTableQuery,sqlConnection)
            sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
            executeDMLQueryOnTarget(strCreateStagingTable,sqlConnection)
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
        df = spark.read.load(path=strSourceTablePath,format='delta')
        df.createOrReplaceTempView(strTempSourceView)
        strCreateTempViewQuery ='CREATE OR REPLACE TEMP VIEW {TempViewName} AS ( SELECT {SelectCols} FROM ( SELECT {SelectCols} {PartitionBy} FROM {SourceTableName} WHERE 1=1 {FilterCondition}) {FilterOnSeq})'
        listColumns = [''+l['SourceColumn']+''+' AS '+l['TargetColumn']+'' for l in listOfMapping]
        listCheckSumColWithDt = [l+' ' +'BIGINT' for l in lstCheckSumColums]
        listAuditColsWithDt = [l+' ' +'TIMESTAMP' for l in lstAuditColumns]
        strSCD1CheckSum,strSCD2CheckSum,strPrimaryColCheckSum = getSCDCheckSumColumns('source')
        strTableCols = ','.join(listColumns)
        # strTableCols = strTableCols+','+strSCD1CheckSum+','+strSCD2CheckSum+',to_timestamp(\''+ETLLastModifiedTimeStamp+'\') AS etllastmodifiedtimestamp'
        strTableCols = strTableCols+','+strSCD1CheckSum+','+strSCD2CheckSum+',etllastmodifiedtimestamp'
        if len(SCD2Cols) > 0:
            strTableCols= strTableCols + ",to_timestamp('"+ValidFromDate+"') AS validfromdate,to_timestamp('2099-12-31 23:59:59') as validtodate, 1 as validflag"
        strFilterCondition = ""
        strFilterCondition = "AND " +strIncrementalColumn +" "+strIncremantalColOperator+" CAST('"+strIncrementalColValue+"' AS "+strIncrementalColDataType+")"
        strPartitionBy = ",ROW_NUMBER() OVER (PARTITION BY "+strPrimaryColCheckSum+ " ORDER BY "+strPartitionOrderByColumn+" DESC) AS Seq"
        strFilterOnSeq = "WHERE Seq=1 "
        # print(strPartitionBy)
        strCreateTempViewQuery = strCreateTempViewQuery.format(TempViewName = strTempViewName,SelectCols = strTableCols,PartitionBy = strPartitionBy\
            ,SourceTableName = strTempSourceView, FilterCondition= strFilterCondition,FilterOnSeq = strFilterOnSeq)
        strCreateTempViewQuery = strCreateTempViewQuery.replace('[','').replace(']','')
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
        strDeleteQuery ="DELETE FROM {TableName} WHERE {ConditionColumns} == 42".format(TableName = strTargetTable,ConditionColumns = strConditionColumn)
        print(strDeleteQuery)
        # df = spark.sql(strDeleteQuery)
        return str(df.collect()[0][0])+" Records deleted due to all columns having NULL data."
    except Exception as e:
        raise Exception(e,"Error in function dqCheck().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def updateIsDelete():
    try:
        strUPdateQuery = f"UPDATE m SET m.isdelete=1 FROM {strTargetTable} m INNER JOIN {strStgTableNameWithSchema} s on m.id = s. id "\
        "where s.recid is null and s.isdelete=1"
        # print(strUPdateQuery)
        return strUPdateQuery
    except Exception as e :
        raise Exception(e,"Error in function updateIsDelete().")
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def deleteRecords():
    try:
        strDeleteQuery = f"DELETE FROM {strTargetTable} WHERE isdelete=1"
        # print(strUPdateQuery)
        return strDeleteQuery
    except Exception as e :
        raise Exception(e,"Error in function deleteRecords().")
        

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
        strTruncateTargetTable = "TRUNCATE TABLE "+strTargetTable
        print(strTruncateTargetTable)
        # spark.sql(strTruncateTargetTable)
        # strTargetTableCreated='0'
        print('\n........ End: Dropping target table for full load.........\n')
    #Comment since all tables are created using separate notebook
    # createSchema()
    # createTable()
    #Comment since all tables are created using separate notebook
    #Create Incremental view for source table.
    createIncrementalView()
    # check if source is hacing data. if source does not have data not need to proceed further. 
    dfSourceView = spark.sql("SELECT * FROM "+strTempViewName)
    print(dfSourceView.count())
    numOfRecordsInSource = dfSourceView.count()
    # numOfRecordsInSource = 1
    if numOfRecordsInSource > 0:
        #Start Logic to insert data in Staging table 
        print('\n........ Start: Truncating Staging Table .........\n')
        strTruncateStagingTable = 'TRUNCATE TABLE '+strStgTableNameWithSchema
        print(strTruncateStagingTable)
        sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
        executeDMLQueryOnTarget(strTruncateStagingTable,sqlConnection)
        print('\n........ End: Truncating Staging Table .........\n')

        print('\n........ Start: Inserting data in Staging Table.........\n')
        pandas_dfSourceView = dfSourceView.toPandas()
        dbEngine = getSQLAlchamyDBEngine(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
        pandas_dfSourceView.to_sql(name = strStgTableNameWithoutSchema,con = dbEngine,schema = strTargetSchemanName , if_exists='append',index = False)
        print('\n........ End: Inserting data in Staging Table.........\n')

        #End Logic to insert data in Staging table 
        if strISMerge == '0':
            print('\n........ Start: Inserting data in Target Table.........\n')
            strInsertQuery = getInsertQuery()
            print(strInsertQuery)
            sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
            executeDMLQueryOnTarget(strInsertQuery,sqlConnection)
            print('\n........ End: Inserting data in Target Table.........\n')
        elif strISMerge == '1':
            strMergeQuery = getMergeQuery()
            print('\n........ Start: Merge Script to Upsert Data ........\n')
            print(strMergeQuery,'\n')
            sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
            executeDMLQueryOnTarget(strMergeQuery,sqlConnection)
            print('\n........ End: Merge Script to Upsert Data ........\n')
            if len(SCD2Cols) > 0:
                print('\n........ Start: Loading SCD2 Data load ........\n')
                ProcessSCD2Data()   
                print('\n........ End: Loading SCD2 Data load ........\n')
        #Logic to update isdelete flag in Main table in SQL Server DB.
        print('\n........ Start: Update isdelete flag .........\n')
        strUpdateIsDelte = updateIsDelete()
        print(strUpdateIsDelte)
        sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
        executeDMLQueryOnTarget(strUpdateIsDelte,sqlConnection)
        print('\n........ End: Update isdelete flag ........\n')

        #Logic to delete records with isdelete =1 in Main table in SQL Server DB.
        print('\n........ Start: Delete Records .........\n')
        strDeleteQuery = deleteRecords()
        print(strDeleteQuery)
        sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
        executeDMLQueryOnTarget(strDeleteQuery,sqlConnection)
        print('\n........ End: Delete Records ........\n')

        
    else:
        processMsg = processMsg +'\n No new data from source.'
    updateIncrementalValue(strTaskID,StartTime)
    _,_,_,endTime,_ = generateTimeStamp()
    LogError(taskid=strTaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget-Sub-SQLDB",process_parameter=processParamter\
        ,process_details=processMsg\
        ,starttime=StartTime,endtime=endTime,status='success',error_log="NULL",dq_log=strDQMessage,source_record_count = str(numOfRecordsInSource))
except Exception as e:
    if len(e.args)>1:
        processMsg =processMsg+e.args[1]
    else:
        processMsg =processMsg+"Issue in main logic."
    msg = str(traceback.format_exc()).replace('"','`').replace("'",'`')
    print(msg)
    _,_,_,endTime,_ = generateTimeStamp()
    LogError(taskid=strTaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget-Sub-SQLDB",process_parameter=processParamter\
        ,process_details=processMsg\
        ,starttime=StartTime,endtime=endTime,status='failed',error_log=msg,dq_log="NULL",source_record_count=source_record_count)

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
