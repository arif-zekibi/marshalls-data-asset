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
# META           "id": "938e5954-d7a9-4061-9819-43d6ae2f3fb9"
# META         },
# META         {
# META           "id": "c2bc7757-3294-426a-a235-3ea7db1e1aea"
# META         },
# META         {
# META           "id": "b914bd3d-f8f9-40d9-a814-9c2c4db325a7"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql('ALTER TABLE lh_silver_correction.d365.generaljournalaccountentry ADD COLUMN accountingcurrencyamount1 DECIMAL(38,6)')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load(path="abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/938e5954-d7a9-4061-9819-43d6ae2f3fb9/Tables/d365/batchjob",format='delta')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.select(['recversion','partition','recid','company'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(['recversion','partition']).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select, with Columns and Case
from pyspark.sql.functions import *
# display(df.withColumn('col1',df['partition']+1000).limit(10))
# display(df.withColumn('new_com',when(df.company == 'dbb','ansari').when(df.company=='DBB','arif').otherwise('husen') ).limit(10))
# df.withColumn('col1',df.company+'arif').withColumn('col2',df.partition).show(truncate=False)
# df.withColumns({'col1':df.company+'arif','col2':df.partition+1000}).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Aggregate
# df.groupBy(df.company).count().show(truncate=False)
# df.groupBy(df.company).avg('partition','recid') .show(truncate=False)
# df.groupBy().sum('partition','recid').show(truncate=False)
# df.groupBy(df['company']).sum('partition','recid').show(truncate=False)
# df.groupBy().count().show()
# df.groupBy(df.company).agg({'partition':'sum'}).show()
# df.groupBy(df.company,df.partition).agg({'recid':'count'}).show()
# df.groupBy(df.company,df.partition).agg(count('recid').alias('col1'),sum('recid').alias('col2')).show()
# df.groupBy(df.company).agg({'recid':'sum'}).show()
# df.groupBy(df['company']).agg(count('recid').alias('col1'),sum('recid').alias('col2')).show()
# df.groupBy(df['company']).agg(count('recid').alias('col1'),sum('recid').alias('col2')).orderBy('company',1).show()
# dfGroup = df.groupBy(df.company).agg(count('*').alias('col1'),sum('recid').alias('col2'))
# dfGroup.sort('company',ascending=[False]).show()
# dfGroup.sort('company','col1',ascending=[1,0]).show()
# dfGroup.sort('col1',ascending=[0]).show()
# dfGroup.orderBy(df['company'].desc()).show()
# dfGroup.columns
# dfGroup.show()
# dfGroup.groupBy(dfGroup.company).agg(count(when(dfGroup.col1 != 1,1).otherwise(None)).alias('RecordCount')).sort('RecordCount',ascending=[False]).show()
# dfGroup.withColumns({'col3':dfGroup.col1+1000,'col4':dfGroup.col2/10000}).show()
dfGroup.withColumn('col3',when((dfGroup.col1 > 1) & (dfGroup.col1 < 10),'Yes').otherwise('No')).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT Id ,COUNT(1) Records FROM dataverse_mrshd365cete_cds2_workspace_unqf9fb2400291e4fce8525104f6a65a.dimensionattributevalue group by Id having count(1) > 1")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.load('abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/c2bc7757-3294-426a-a235-3ea7db1e1aea/Tables/dimensionattributevalue',format='delta')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from  pyspark.sql.functions import *
df_id = df.select(df.Id,hash(df.Id).alias('hashid'),xxhash64(df.Id).alias('xxhash64id'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = df_id.groupBy(df_id.xxhash64id).agg(count('xxhash64id').alias('Rows'))
df2.filter(df2.Rows > 1).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_id.filter(df_id.hashid=='-1711699036').show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsecurityduties')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsecurityprivileges')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsecurityreference')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.martablefieldsinformationtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.martableinformationtable')






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendonholdhistory')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventitemgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetobjecttable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.agreementheader')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmporttable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projbudget')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventitemgroupitem')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetworkordertype')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.insenumvaluetable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dirpartylocation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresstoragedimensiongroupitem')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.bomcalctrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodcalctrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattributevaluegroupcombination')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.markupautotable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmcontainers')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marcreditlimitsource')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.logisticsaddresscounty')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.tmsroutetable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctrprodrouteactivity')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.hcmpositionworkerassignment')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custinvoicetrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.gupretailchannelinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresproduct')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.bom')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.purchtablelinks')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctrtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventsite')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodparmreportfinished')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionruleappliedhierarchy')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctrcapres')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.taxgroupdata')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.destinationcode')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.mainaccount')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresintvalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodtablejour')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetfunctionallocationtype')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.catalogproductinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresdistinctproductvariant')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattributevaluederiveddimensions')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetjobtrade')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmlinecosttranslink')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.logisticsaddresszipcode')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionconstraintnodecriteria')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.hcmpositiondetail')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattributevaluecombination')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projitemtrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.costcontroltranscommittedcost')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.budgetplanscenario')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.budgettransactionheader')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.omteam')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionrulecriteria')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctrresourcegroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodroute')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendpackingslipjour')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.retailpricingsimulatorinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsstransactionsendtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.markupautoline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.retailcatalogprodinternalorginstanceval')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctractivityresourcerequirement')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventitemsalessetup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.report')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ledger')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionconstraintnode')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marapprovedlimithistory')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dirpersonname')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.workflowworkitemtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.taxregistration')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.purchline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dataarea')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.bomcalctable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendpackingsliptranshistory')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projpostedtranstable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.companyinfo')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custinvoicejour')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.whsloadtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custpackingsliptrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.bomcostgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.martableinformationtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.bomversion')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.credmancreditlimitcustgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresproductinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.retailchannelinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.exchangerate')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecorescategoryinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresdistinctproduct')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecorestextvalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.route')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmcommoditycodetable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.logisticsaddresscountryregion')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.assetbooktable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custdisputehistory')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dirorganization')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctractivityrequirementset')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecorescategory')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.assetgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.pccomponentinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.unitofmeasuretranslation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventjournalname')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.pricedisctable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetworkorderline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custclassificationgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ledgerjournaltrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresstoragedimensiongroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ins_tablerelationshipsdata')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.taxtrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventitempurchsetup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresproductcategory')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendinvoicejour')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.unitofmeasureconversion')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsecurityprivileges')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projcosttranscost')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendbankaccount')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.caseassociation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventsum')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.routecostcategoryprice')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresreleasedengineeringproductversionattributeinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetfunctionallocation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.taxonitem')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.pcclass')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmcostinvoicetrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dirperson')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmcostallocatetrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.logisticselectronicaddress')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventitemprice')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.tmsroute')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.exchangeratecurrencypair')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.bomtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresproducttranslation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.assetbook')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.purchtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.securityuserrole')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.casedetail')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.sysuserinfo')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.workflowtrackingstatustable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionhierarchy')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.unitofmeasure')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattrvalueledgeroverride')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresengineeringproductcategoryattributeinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projtransposting')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattributevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projcosttrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.tmsloadbuildstrategyattribvalueset')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresattributetype')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.paymterm')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodjournalprod')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.docuref')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.credmancreditlimitcustgroupline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.martradingoffer')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.resresourceidentifier')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.assettable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.agreementlinequantitycommitment')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.logisticslocation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.generaljournalaccountentry')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendpackingslipversion')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marapprovedlimit')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendinvoicetrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.martablefieldsinformationtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresreferencevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.lineofbusiness')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetworkorderservicelevel')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projforecastcost')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendinvoicepackingslipquantitymatch')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.retailsalestableinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marprodrejectreasons')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventsettlement')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.purchagreementheader')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.whsloadline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.costingversion')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsecurityroles')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.budgettransactionline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresbooleanvalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.whsshipmenttable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.salesagreementheader')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.agreementlinevolumecommitment')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.hcmemployment')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventtrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.batchjob')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.fiscalcalendarperiod')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionhierarchylevel')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.taxdata')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmcosttrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.workflowtrackingtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresattributevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.pricediscgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctractivityrequirement')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventmodelgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventtablemodule')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.hcmfmlacasedetail')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.reasontableref')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projbudgetrevisionline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projbudgetrevision')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetworkergroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projempltranscost')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsecurityduties')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dirpartytable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendpurchorderjour')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.insenumidtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.salestable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodroutetrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventitembarcode')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendpackingsliptrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.gupsalestableinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.customerinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.martradingdivision')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendtransopen')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresattribute')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattributevaluegroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventtransferjour')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.logisticsaddressstate')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.budgetplanheader')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctractivity')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.generaljournalentry')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.budgetplanline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattributevalueset')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresproductmaster')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresdatetimevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projbudgetline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marcontainerpo')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsalesoffice')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dirpartyrelationship')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marrecordrejectreasons')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.reasontable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventitempricesim')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventjournaltrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsecurityreference')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.exchangeratetype')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventtransposting')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.hcmworker')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.userinfo')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctrrouteopractivity')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendtrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmcosttypetable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventitemlocation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattribute')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.purchreqtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattributelevelvalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dlvmode')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.casecategoryhierarchydetail')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetworkordertable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.casedetailbase')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.guprebatedateinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.retailinternalorgproductinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.wrkctrresourcegroupresource')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresvalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventiteminventsetup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodjournaltable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventjournaltable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventlocation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.entassetrequesttable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.assettrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custcollectionscasedetail')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ominternalorganization')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.marsstransactionrecvtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventtransorigin')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventdim')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.workflowelementtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecorescurrencyvalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.purchreqline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.routeversion')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecorescategoryattributelookup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custtrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmgoodsintransitorder')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.guppricetreeinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.reqitemtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.salesline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custcollectionsagent')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionrule')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventmodelgroupitem')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ecoresfloatvalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.vendtable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodpool')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.omoperatingunit')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.eeuserrolechangelog')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.prodjournalroute')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.smmbusrelsegmentgroup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.costsheetcalculationfactor')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.routetable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.routecostcategory')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dirpersonuser')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.agreementline')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.costsheetcalculationbasis')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custvendexternalitem')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.assetgroupbooksetup')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.workflowtrackingcommenttable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.credmanaccountstatustable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.gupsalesquotationinstancevalue')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custcollectionsagentpool')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projitemtranscost')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.ledgerjournaltable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.inventposting')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dirorganizationbase')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionattributevaluesetitem')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.itmcontaineractivitytable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.logisticsaddresscountryregiontranslation')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.custcollectionspool')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projcategory')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.routeopr')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.costsheetnodetable')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.logisticspostaladdress')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.projempltrans')
spark.sql('DROP TABLE IF EXISTS lh_silver_correction.d365.dimensionfinancialtag')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# lh_bronze_raw Path
# abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw
#Silver layer path
# abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/938e5954-d7a9-4061-9819-43d6ae2f3fb9/Tables/d365/assettrans
df = spark.read.load(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/938e5954-d7a9-4061-9819-43d6ae2f3fb9/Tables/d365/assettrans",format = 'delta' )
print(df.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
# # df.write.mode('overwrite').parquet(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans/2025-03-04T02:16:01.6883193.parquet")
# df.write.partitionBy.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")
emt = '2025-03-04T04:16:01.6883193'
df = (
        df
        .withColumn('etllastmodifiedtimestamp_new',F.lit(emt).cast('timestamp'))
        .withColumn('Year',F.year(F.lit(emt).cast('timestamp')))
        .withColumn('Month',F.month(F.lit(emt).cast('timestamp')))
        .withColumn('Day',F.day(F.lit(emt).cast('timestamp')))
    )
df.write.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
# # df.write.mode('overwrite').parquet(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans/2025-03-04T02:16:01.6883193.parquet")
# df.write.partitionBy.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")
emt = '2025-03-03T04:16:01.6883193'
df = (
        df
        .withColumn('etllastmodifiedtimestamp_new',F.lit(emt).cast('timestamp'))
        .withColumn('Year',F.year(F.lit(emt).cast('timestamp')))
        .withColumn('Month',F.month(F.lit(emt).cast('timestamp')))
        .withColumn('Day',F.day(F.lit(emt).cast('timestamp')))
    )
df.write.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_Df = spark.read.load(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans",format='parquet')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(new_Df.groupBy('etllastmodifiedtimestamp_new').count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T
# # df.write.mode('overwrite').parquet(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans/2025-03-04T02:16:01.6883193.parquet")
# df.write.partitionBy.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")
emt = '2025-03-04T04:16:01.6883193'
df = (
        df
        .withColumn('etllastmodifiedtimestamp_new',F.lit(emt).cast('timestamp'))
        .withColumn('Year',F.year(F.lit(emt).cast('timestamp')))
        .withColumn('Month',F.month(F.lit(emt).cast('timestamp')))
        .withColumn('Day',F.day(F.lit(emt).cast('timestamp')))
    )
df.write.partitionBy(('Year','Month','Day')).mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # df.write.mode('overwrite').parquet(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans/2025-03-04T02:16:01.6883193.parquet")
# df.write.partitionBy.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")
emt = '2025-03-01T04:16:01.6883193'
df = (
        df
        .withColumn('etllastmodifiedtimestamp_new',F.lit(emt).cast('timestamp'))
        .withColumn('Year',F.year(F.lit(emt).cast('timestamp')))
        .withColumn('Month',F.month(F.lit(emt).cast('timestamp')))
        .withColumn('Day',F.day(F.lit(emt).cast('timestamp')))
    )
df.write.partitionBy(('Year','Month','Day')).mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # df.write.mode('overwrite').parquet(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans/2025-03-04T02:16:01.6883193.parquet")
# df.write.partitionBy.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")
emt = '2025-03-02T04:16:01.6883193'
df = (
        df
        .withColumn('etllastmodifiedtimestamp_new',F.lit(emt).cast('timestamp'))
        .withColumn('Year',F.year(F.lit(emt).cast('timestamp')))
        .withColumn('Month',F.month(F.lit(emt).cast('timestamp')))
        .withColumn('Day',F.day(F.lit(emt).cast('timestamp')))
    )
df.write.partitionBy(('Year','Month','Day')).mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # df.write.mode('overwrite').parquet(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans/2025-03-04T02:16:01.6883193.parquet")
# df.write.partitionBy.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")
emt = '2025-03-03T04:16:01.6883193'
df = (
        df
        .withColumn('etllastmodifiedtimestamp_new',F.lit(emt).cast('timestamp'))
        .withColumn('Year',F.year(F.lit(emt).cast('timestamp')))
        .withColumn('Month',F.month(F.lit(emt).cast('timestamp')))
        .withColumn('Day',F.day(F.lit(emt).cast('timestamp')))
    )
df.write.partitionBy(('Year','Month','Day')).mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_Df = spark.read.load(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans",format='parquet')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(new_Df.groupBy('etllastmodifiedtimestamp').count())
from pyspark.sql import functions as f 
# display(new_Df.groupBy('Id').agg(f.count('*')))
display(new_Df.groupBy('Id').agg(f.count('*')).select('Id').count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(new_Df.filter(F.col('etllastmodifiedtimestamp_new') > '2025-03-03 02:16:01.688319').groupBy('etllastmodifiedtimestamp_new').count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(new_Df.filter((F.col('Year')==2025) & (F.col('Month') == 3) & (F.col('Day') == 3) & (F.col('etllastmodifiedtimestamp_new') > '2025-03-03 02:16:01.688319')).groupBy('etllastmodifiedtimestamp_new').count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('etllastmodifiedtimestamp_new',F.lit('2025-03-02T04:16:01.6883193').cast('timestamp'))
df.repartition(1).write.partitionBy('etllastmodifiedtimestamp_new').mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_Df = spark.read.load(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans",format='parquet')
display(new_Df.filter(F.col('etllastmodifiedtimestamp_new') > '2025-02-28T00:16:01.6883193').groupBy('etllastmodifiedtimestamp_new').count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn('etllastmodifiedtimestamp_new',F.lit('2025-03-01T04:16:01.6883193').cast('timestamp'))
df.repartition(1).write.mode('append').format('parquet').save(path = "abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/agilepoint_dbo_assettrans")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils.mssparkutils
for f in mssparkutils.fs.ls('abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/938e5954-d7a9-4061-9819-43d6ae2f3fb9/Tables/d365'):
    # print(f.path)
    print(mssparkutils.fs.ls(f.path+'/_delta_log'))
    df = spark.read.json(f.path+'/_delta_log')
    break

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add etllastmodifiedtimestamp 
df = spark.read.load(path='abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/2025-03-14T08:48:54.2029438',format='parquet')

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

print(df.count(),df2.count())
# etllastmodifiedtimestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adding timestamp column and checking schema evolution
from pyspark.sql import functions as f
# df2 = df2.withColumn('etllastmodifiedtimestamp',f.lit('2025-03-14T09:03:58.0110053').cast('timestamp'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df2.repartition(1).write.mode('overwrite').parquet(path='abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/2025-03-14T09:03:58.0110053.parquet')
df2.coalesce(1).write.mode('overwrite').parquet(path='abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/')
# format('parquet').save

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getDirandFileName (strFilePath):
    details = os.path.split(strFilePath)
    return details[0],details[1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
try:
    orgFilePath = 'abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/2025-03-14T09:03:58.0110053.parquet'
    orgFileDir,orgFileName = getDirandFileName(orgFilePath)
    newFilePath = orgFileDir+'/temp'
    df = spark.read.load(path=orgFilePath,format='parquet')
    df = df.withColumn('etllastmodifiedtimestamp',f.lit('2025-03-14T09:03:58.0110053').cast('timestamp'))
    df.repartition(1).write.mode('overwrite').parquet(path = newFilePath)
    print(df.columns)
    newFiles = notebookutils.fs.ls(newFilePath)
    for file in newFiles:
        # print(file.path)
        # print(os.path.splitext(file.path)[1])
        if os.path.splitext(file.path)[1] == '.parquet':
            print('Filemove in progress..!!')
            notebookutils.fs.rm(orgFilePath)
            notebookutils.fs.cp(file.path,orgFileDir+'/NEW_'+orgFileName)
            # notebookutils.fs.cp(file.path,orgFilePath)

except Exception as e:
 raise e

# notebookutils.fs.ls('abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/') 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.fs.cp('abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/temp/part-00000-5f46e18e-b52a-41cc-8e8c-0010dd9df875-c000.snappy.parquet','abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/2025-03-14T09:03:58.0110053.parquet')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# orgFilePath = 'abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/2025-03-14T09:03:58.0110053.parquet'
orgFilePath = 'abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/NEW_2025-03-14T09:03:58.0110053.parquet'
orgFileFolder = 'abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u'
newFIle = 'abfss://0570d2f2-4789-4601-91b2-caceb33c9ce2@onelake.dfs.fabric.microsoft.com/b914bd3d-f8f9-40d9-a814-9c2c4db325a7/Files/lh_bronze_raw/mssql/agilepoint/AP_Dev_DataEntities_DB_MSP_ENEConsumption__u/temp/'
# spark.read.option("mergeSchema", "true").parquet("data/test_table")
df3 = spark.read.option("mergeSchema","true").load(path=orgFilePath,format='parquet')
df4 = spark.read.option("mergeSchema","true").load(path = orgFileFolder,format='parquet')
df2 = spark.read.load(path=newFIle,format='parquet')
print(df3.columns)
print(df2.columns)
display(df3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df4.groupBy('etllastmodifiedtimestamp').count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df.columns)
print(df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
