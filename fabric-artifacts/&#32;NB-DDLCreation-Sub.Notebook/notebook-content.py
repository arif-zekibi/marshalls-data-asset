# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
TaskID ="-1721482531"
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
        ",CASE is_nullable when 0 then 'NOT NULL' else '' end as NullConstraint "\
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
    LogError(taskid=TaskID,Lineage_id=Lineage_id,processed_by="NB-DDLCreation-Sub",process_parameter=""\
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
        listColumnMapping = DfTaskDetails.select(['SourceColumn','SourceColumnDataType','TargetColumn','TargetColumnDataType','is_primary','is_scd1','is_scd2','NullConstraint']).toPandas().to_dict(orient='records')
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

#DDL Functions for SQL Server DB
def createTableSQLServerDB():
    try:
        # if not spark.catalog.tableExists(strTargetTable):
        print('\n........ Start:Create Target Table('+strTargetTable+')........\n')
        strCreateTableQueryFormat ="IF NOT EXISTS (SELECT T.NAME,S.NAME FROM SYS.TABLES T INNER JOIN SYS.SCHEMAS S ON T.SCHEMA_ID = S.SCHEMA_ID WHERE T.NAME='{TableNameWithOutSchema}' AND S.NAME='"+strTargetSchemanName+"') BEGIN CREATE TABLE {TableName} ({Columns}) END"
        listColumns = ['['+l['TargetColumn']+'] '+l['TargetColumnDataType']+' '+l['NullConstraint'] for l in listOfMapping]
        listCheckSumColWithDt = ['['+l+'] ' +'BIGINT' for l in lstCheckSumColums]
        listIDColWithDt = ['['+l+'] ' +'BIGINT' for l in lstIDColumns]
        listAuditColsWithDt = ['['+l+'] ' +'DATETIME2' for l in lstAuditColumns]
        listColumns.extend(listCheckSumColWithDt)
        listColumns.extend(listIDColWithDt)
        listColumns.extend(listAuditColsWithDt)
        if len(SCD2Cols) > 0:
            listColumns.append('[validfromdate] datetime2')
            listColumns.append('[validtodate] datetime2')
            listColumns.append('[validflag] TINYINT')
        strTableCols = ' ,'.join(listColumns)
        strCreateTableQuery = strCreateTableQueryFormat.format(TableNameWithOutSchema = strDoTableName, TableName=strTargetTable,Columns = strTableCols)
        print(strCreateTableQuery,'\n')
        strCreateStagingTable = strCreateTableQueryFormat.format(TableNameWithOutSchema = strStgTableNameWithoutSchema,TableName=strStgTableNameWithSchema,Columns = strTableCols)
        #For Stagging table there must not be any contraint
        strCreateStagingTable = strCreateStagingTable.replace('NOT NULL','')
        #For Stagging table there must not be any contraint
        print(strCreateStagingTable)
        sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
        executeDMLQueryOnTarget(strCreateTableQuery,sqlConnection)
        sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
        executeDMLQueryOnTarget(strCreateStagingTable,sqlConnection)
        updateObjectCreatedStatus(strTargetTableId,'1')
        print('\n........ End:Create Target Table('+strTargetTable+')........\n')
    except Exception as e:
        raise Exception(e,"Error in function createTable().")

def createSchemaSQLServerDB():
    try:
        print('\n........ Start:Create Target SQL Schema('+strTargetSchemanName+')........\n')
        strCreateSchema = 'CREATE SCHEMA '+strTargetSchemanName
        print(strCreateSchema)
        sqlConnection = getAzureConnectionUsingProperties(ServerName=strTargetURL,UserName=strTargetUserName,Password=strTargetPassword,DatabaseName=strTargetDatabaseName)
        executeDMLQueryOnTarget(strCreateSchema,sqlConnection)
        print('\n........ End:Create Target SQL Schema('+strTargetSchemanName+')........\n')
    except Exception as e:
        print('\n........Schema('+strTargetSchemanName+') is already created........\n')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    if DatabaseType == 'sqlserverdb': 
        if strTargetTableCreated == '0':
            # createSchemaSQLServerDB()
            createTableSQLServerDB()
        else:
            print('\n........Schema('+strTargetSchemanName+') is already created........\n')
            print('\n........Table('+strTargetTable+') is already created........\n')
except Exception as e:
    if len(e.args)>1:
        processMsg =processMsg+e.args[1]
    else:
        processMsg =processMsg+"Issue in main logic."
    msg = str(traceback.format_exc()).replace('"','`').replace("'",'`')
    print(msg)
    _,_,_,endTime,_ = generateTimeStamp()
    LogError(taskid=strTaskID,Lineage_id=Lineage_id,processed_by=" NB-DDLCreation-Sub",process_parameter=processParamter\
        ,process_details=processMsg\
        ,starttime=StartTime,endtime=endTime,status='failed',error_log=msg,dq_log="NULL",source_record_count="NULL")

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
