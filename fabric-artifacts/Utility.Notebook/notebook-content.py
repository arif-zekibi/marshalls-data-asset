# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# CELL ********************

import datetime
from pytz import timezone
import pyodbc
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getAzureConnection():
    try:
        server_name = "marshallsdmzdev.database.windows.net"
        username = "azuremetadatadbuser"
        password = "ITnBUco/JOjJmbe3hafRtJRbWsCTdUTKHvaFySizRRg=" # Please specify password here
        database_name = "MetadataDB"
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server_name+';'\
        'DATABASE='+database_name+';UID='+username+';PWD='+ password)
        return cnxn
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getAzureConnectionUsingProperties(ServerName,UserName,Password,DatabaseName):
    try:
        server_name = ServerName
        username = UserName
        password = Password # Please specify password here
        database_name = DatabaseName
        cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server_name+';'\
        'DATABASE='+database_name+';UID='+username+';PWD='+ password)
        return cnxn
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getSQLAlchamyDBEngine(ServerName,UserName,Password,DatabaseName):
    try:
        connection_string = "DRIVER={ODBC Driver 18 for SQL Server};SERVER="+ServerName+";DATABASE="+DatabaseName+";UID="+UserName+";PWD="+Password
        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": connection_string}
        )
        engine = create_engine(connection_url)
        return engine
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def LogError(taskid,Lineage_id,processed_by,process_parameter,process_details,starttime,endtime,status,error_log,dq_log = "NULL",source_record_count="NULL"):
    print('Inserting Log......!!')
    # args = {}
    # args['task_id'] =taskid
    # args['lineage_id'] =Lineage_id
    # args['processed_by'] =processed_by
    # args['process_parameter'] = process_parameter
    # args['process_details'] = process_details
    # args['starttime'] = starttime
    # args['endtime'] = endtime
    # args['source_record_count'] = '0'
    # args['target_record_count'] = '0'
    # args['record_volumn_kb'] ='0'
    # args['status']  = status
    # args['error_log'] = error_log
    # args['inserted_by'] = 'spark'
    # args['modified_by'] = 'spark'
    strQuery = "INSERT INTO dbo.log([task_id],[lineage_id],[processed_by],[processed_parameter],[process_details],[starttime],[endtime],[status],[error_log],[dq_log],[inserted_by],[modified_by],[source_record_count])"\
                "VALUES('"+taskid+"','"+Lineage_id+"','"+processed_by+"','"+process_parameter+"','"+process_details+"','"+starttime+"','"+endtime+"','"+status+"','"+error_log+"','"+dq_log+"','spark','spark','"+source_record_count+"')"
    strQuery = strQuery.replace("'NULL'","NULL")
    executeInsertQuery(strQuery)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def executeSelectQuery(strQuery:str):
    try:
        cnxn = getAzureConnection()
        cursor = cnxn.cursor()
        df_pd = pd.read_sql(strQuery,cnxn)     
        if (df_pd.shape[0]>0):
            dfResult = spark.createDataFrame(df_pd)
        else:
            dfResult = None
        cursor.close()
        return dfResult
    except Exception as err:
        raise err

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def executeInsertQuery(strQuery:str):
    try:
        cnxn = getAzureConnection()
        cursor = cnxn.cursor()
        cursor.execute(strQuery)
        cnxn.commit()
        cursor.close()
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def executeDMLQuery(strQuery:str):
    try:
        cnxn = getAzureConnection()
        cursor = cnxn.cursor()
        cursor.execute(strQuery)
        cnxn.commit()
        cursor.close()
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def executeDMLQueryOnTarget(strQuery:str,sqlConnection):
    try:
        cnxn = sqlConnection
        cursor = cnxn.cursor()
        cursor.execute(strQuery)
        cnxn.commit()
        cursor.close()
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create ETLLastModifiedTimeStamp,ValidFromDate and ValidToDate 
def generateTimeStamp():
    try:
        format = "%Y-%m-%d %H:%M:%S.%f"
        ETLLastModifiedTimeStamp =  datetime.datetime.utcnow().astimezone(timezone('Europe/London')).strftime(format)
        format = "%Y-%m-%d %H:%M:%S"
        ValidToDate = (datetime.datetime.utcnow().astimezone(timezone('Europe/London'))+datetime.timedelta(seconds=-1)).strftime(format)
        ValidFromDate= datetime.datetime.utcnow().astimezone(timezone('Europe/London')).strftime(format)
        TimeStamp =  ETLLastModifiedTimeStamp
        format = "%Y_%m_%d_%H_%M_%S"
        ViewTimeStamp= datetime.datetime.utcnow().astimezone(timezone('Europe/London')).strftime(format)
        print(ETLLastModifiedTimeStamp)
        print(ValidFromDate)
        print(ValidToDate)
        return ETLLastModifiedTimeStamp,ValidFromDate,ValidToDate,TimeStamp,ViewTimeStamp
    except Exception as e:
        raise Exception(e,"Error in generateTimeStamp().")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def updateIncrementalValue(strTaskID:str,strValue:str):
    try:
        strUpdateQuery = "UPDATE dbo.task SET incremental_value='"+strValue+"' WHERE task_id="+strTaskID
        cnxn = getAzureConnection()
        cursor = cnxn.cursor()
        cursor.execute(strUpdateQuery)
        cnxn.commit()
        cnxn.close()
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def updateObjectCreatedStatus(strObjectID:str,strValue:str):
    try:
        strUpdateQuery = "UPDATE dbo.object SET iscreated="+strValue+" WHERE object_id="+strObjectID
        cnxn = getAzureConnection()
        cursor = cnxn.cursor()
        cursor.execute(strUpdateQuery)
        cnxn.commit()
        cnxn.close()
    except Exception as e:
        raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
