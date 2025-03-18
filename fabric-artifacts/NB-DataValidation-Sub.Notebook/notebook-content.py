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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
TaskID ="-1659280746"
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

from pyspark.sql.functions import hash

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
        "ELSE LOWER(CONCAT(so.database_name,'.'+so.schema_name,'.'+so.object_name)) END "\
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
        ",so_con.connection_type as so_connectiontype"\
        ",do_con.connection_type as do_connectiontype "\
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
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-DataValidation-Sub",process_parameter=""\
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
    strsoconnectiontype = str(dfRow['so_connectiontype'])
    strdoconnectiontype = str(dfRow['do_connectiontype'])
    print(ViewTimeStamp,strTempViewName,strWriteMode,strUpdateType,strSourceTable,strTargetTable,strIncrementalColumn,strIncrementalColDataType,strIncremantalColOperator,strIncrementalColValue,strInsertType\
    ,strSourceDatabaseName,strSourceSchemaName,strSourceTablePath,strTargetDatabaseName,strTargetSchemanName,strTargetTablePath,strSourceURL,strTargetURL
    ,strSoTableName,strDoTableName,strTempTableName,strSourceTableId,strTargetTableId,strsoconnectiontype,strdoconnectiontype)
    # listOfMapping
except Exception as e:
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-LoadDataSourceToTarget",process_parameter=""\
        ,process_details="Error while initializing variables."\
        ,starttime=StartTime,endtime="NULL",status='failed',error_log=str(e),dq_log="NULL",source_record_count=source_record_count)

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
        #'hash(`UPRN`,`validfromdate`) as dwh_id'
        strPrimaryKey,lstPrimaryKey = getPrimaryKey(tableType)
        tableType = 'SourceColumn' if tableType =='source' else 'TargetColumn' 
        SCD1CheckSum = [col[tableType] for col in listOfMapping if col['is_primary'] =='0' and col['is_scd1']=='1' and col[tableType] != '`etllastmodifiedtimestamp`']
        SCD2CheckSum = [col[tableType] for col in listOfMapping if col['is_primary'] =='0' and col['is_scd2']=='1' and col[tableType] != '`etllastmodifiedtimestamp`']
        SCD1CheckSum.sort()
        SCD2CheckSum.sort()
        strSCD1CheckSum ='hash('+",".join(SCD1CheckSum)+') as scd1checksum' if len(SCD1CheckSum)>0 else "hash('') as scd1checksum"
        strSCD2CheckSum='hash('+",".join(SCD2CheckSum)+') as scd2checksum' if len(SCD2CheckSum)>0 else "hash('') as scd2checksum"
        lstColForDuplicateCheck = []
        if len(lstPrimaryKey) > 0:
            lstColForDuplicateCheck = lstPrimaryKey
        else:
            lstColForDuplicateCheck = [l['SourceColumn'] for l in listOfMapping]
        lstColForDuplicateCheck.sort()
        strPrimaryColCheckSum='hash('+",".join(lstColForDuplicateCheck)+')' if len(lstColForDuplicateCheck)>0 else "hash('')"
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
        strSurrogateKey = 'hash('+",".join(listPrimarKey)+') as dwh_id'
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

def GetSparkDataFrame(strConnectionType:str,strTableName:str):
    # strConnectionType = 'lakehouse'
    # strTableName = strSourceTable
    excludeCols = ['sinkmodifiedon','id','sinkcreatedon']
    try:
        if strConnectionType == 'lakehouse':
            strSelectQuery = "SELECT {HashColumn} FROM {TableName}"
            listCols = []
            for c in listOfMapping:
                if c['SourceColumn'] not in excludeCols:
                    listCols.append(c['SourceColumn'])
            strHashColumn = ','.join(listCols)
            strSelectQuery = strSelectQuery.format(HashColumn = strHashColumn,TableName = strTableName)
            print(strSelectQuery)
            return spark.sql(strSelectQuery)
    except Exception as e:
        raise Exception(e,"Error in function PrepareQuery().")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfSource = GetSparkDataFrame('lakehouse',strSourceTable)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfSource.select(hash(getColumnsFromMapping('ALL','source',",",ReplaceETLModfiledDate=False))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(tuple(getColumnsFromMapping('ALL','source',",",ReplaceETLModfiledDate=False)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

getColumnsFromMapping('ALL','target',",",ReplaceETLModfiledDate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

getColumnsFromMapping('ALL','source',",",ReplaceETLModfiledDate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
