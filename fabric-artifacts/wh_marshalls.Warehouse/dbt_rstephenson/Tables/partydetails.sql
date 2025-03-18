CREATE TABLE [dbt_rstephenson].[partydetails] (

	[partyrecid] bigint NULL, 
	[partylanguage] varchar(8000) NULL, 
	[partyname] varchar(8000) NULL, 
	[partynumber] varchar(8000) NULL, 
	[instancerelationtype] bigint NULL, 
	[partytype] varchar(14) NOT NULL, 
	[primaryaddresslocation] bigint NULL, 
	[primarycontactemail] bigint NULL, 
	[primarycontactphone] bigint NULL, 
	[email] varchar(8000) NULL, 
	[phone] varchar(8000) NULL, 
	[fax] varchar(8000) NULL, 
	[street] varchar(8000) NULL, 
	[city] varchar(8000) NULL, 
	[countyid] varchar(8000) NULL, 
	[county] varchar(8000) NULL, 
	[stateid] varchar(8000) NULL, 
	[state] varchar(8000) NULL, 
	[zipcode] varchar(8000) NULL, 
	[isocode] varchar(8000) NULL, 
	[bi_postcode] varchar(8000) NULL, 
	[countryregionid] varchar(8000) NULL, 
	[country] varchar(8000) NULL, 
	[address] varchar(8000) NULL, 
	[latitude] decimal(38,10) NULL, 
	[longitude] decimal(38,10) NULL, 
	[country_native] varchar(8000) NULL, 
	[address_native] varchar(8000) NULL, 
	[namealias] varchar(8000) NULL, 
	[partition] bigint NULL, 
	[orgnumber] int NULL
);

