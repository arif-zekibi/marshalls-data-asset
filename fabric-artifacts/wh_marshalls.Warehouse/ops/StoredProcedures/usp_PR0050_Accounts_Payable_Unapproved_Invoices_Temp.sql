CREATE Procedure [ops].[usp_PR0050_Accounts_Payable_Unapproved_Invoices_Temp]
 
 /*********************************************************************************************************************
 ** Name     : [usp_PR0050_Accounts_Payable_Unapproved_Invoices_SiteParameter]
 ** Desc     : Selects financial transactions from table LEDGERJOURNALTRANS where invoices have not been approved.         
 **			  That is Invoices been received but have not been matched to a PO Receipt.
 ** Author   : Arifhusen Ansari
 ** Created  : 11/12/2024(dd/mm/yyyy)
																								
 [ops].[usp_PR0050_Accounts_Payable_Unapproved_Invoices_Temp] 'DBR','2024-02-29','2024-12-31','8401I'
 ***********************************************************************************************************************/

 @company varchar (10),
 @transto datetime,
 @dueto	datetime,
 @ledgeracc	varchar(20)
 
 AS
 SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
 SET NOCOUNT ON;

 BEGIN 

	DROP TABLE IF EXISTS ops.temp_max1
	DROP TABLE IF EXISTS ops.vendtrans
	DROP TABLE IF EXISTS ops.temp_lt
	DROP TABLE IF EXISTS ops.temp_paid 
	DROP TABLE IF EXISTS ops.temp_vendtrans

	---- Start Declare parameter for script execution. 
	--DECLARE @company varchar (10) ='DBR'
	--DECLARE @transto datetime = '2020-02-29'
	--DECLARE @dueto	datetime = '2020-12-31'
	--DECLARE @LedgerAcc	varchar(20) ='8401I'
	---- End Declare parameter for script execution. 

	--Get time stamp for temp table.
	DECLARE @timestamp VARCHAR(14) = (SELECT FORMAT(GETDATE(),'yyyyMMddhhmmss'))
	DECLARE @createlt NVARCHAR(4000)
	
	set @createlt = 'create table ops.temp_lt(
		voucher varchar(50)
		,accountdate date
		,documentdate date
		,currencycode varchar(5)
		,amountcur decimal(10,2)
		,amountmst decimal(10,2)
		,dataareaid varchar(10)
		,ledgeraccount varchar(50)
	)'
	
	EXECUTE sp_executesql @createlt
	insert into ops.temp_lt (voucher,accountdate,documentdate,currencycode,amountcur,amountmst,dataareaid,ledgeraccount)
	SELECT --provided by AW for AxV6 Upgrade for R1496
		je.subledgervoucher as 'voucher',
		convert(date,je.accountingdate) as 'accountdate',
		convert(date,je.documentdate) as 'documentdate',
		ae.transactioncurrencycode   as 'currencycode',
		ae.transactioncurrencyamount  as 'amountcur',
		ae.accountingcurrencyamount  as 'amountmst',
		je.subledgervoucherdataareaid as 'dataareaid',
		ae.ledgeraccount as 'ledgeraccount'
	FROM
		lh_silver_correction.d365.generaljournalaccountentry ae
	INNER JOIN 
		lh_silver_correction.d365.generaljournalentry je
	ON
		ae.generaljournalentry = je.recid
	WHERE 
		je.subledgervoucherdataareaid = @company
	AND  je.subledgervoucherdataareaid not in ('DGJ','DGK')
	 
	 
	 
	-- Create max1:
	DECLARE @createmax1 NVARCHAR(4000)
	SET @createmax1 = 'create table ops.temp_max1(
		voucher varchar(50)
		,dataareaid varchar(10)
	)'
	EXECUTE sp_executesql @createmax1
	
	INSERT INTO ops.temp_max1 (voucher,dataareaid)
	SELECT 
		tab.dataareaid,
		tra.voucher
	FROM	
		lh_silver_correction.d365.ledgerjournaltable tab
	INNER JOIN
		lh_silver_correction.d365.ledgerjournaltrans tra
	ON
		tra.journalnum = tab.journalnum
		and tra.dataareaid = tab.dataareaid
	WHERE
		tra.accounttype = 2   -- vendor
		and tab.journaltype = 2
		and tra.voucher like 'PZ%'
		and  tab.dataareaid not in ('DGJ','DGK')
	GROUP BY
		tab.dataareaid,
		tra.voucher
	------------------------------------------------------------
	--Get all the paid invoices
	-- Create max1:
	DECLARE @createpaid NVARCHAR(4000)
	SET @createpaid = 'create table ops.temp_paid(
		journalnum varchar(50)
		,voucher varchar(50)
		,value decimal(10,2)
	)'
	EXECUTE sp_executesql @createpaid
	
	INSERT INTO ops.temp_paid(journalnum,voucher,value)
	Select ljtr.journalnum, vt.voucher,  sum(vt.amountmst) as value
				From lh_silver_correction.d365.ledgerjournaltrans ljtr
					JOIN lh_silver_correction.d365.vendtrans vt		
					ON
						ljtr.vendtransid = vt.recid
					where closed < @transto
					and left(ljtr.voucher,2) = 'PV'
	group by ljtr.journalnum, vt.voucher

	--create temp table for queries below
	-- Create max1:
	DECLARE @createtmpvendtrans NVARCHAR(4000)
	SET @createtmpvendtrans = 'create table ops.temp_vendtrans(
		invoice varchar(50)
		,accountnum varchar(50)
		,value decimal(10,2)
	)'
	EXECUTE sp_executesql @createtmpvendtrans
	
	INSERT INTO ops.temp_vendtrans(invoice,accountnum,value)
	select invoice, accountnum ,sum(amountmst) as value
	
	from lh_silver_correction.d365.vendtrans --, 
	where closed < @transto
	and isnull(invoice,'') <> ''
	group by invoice, accountnum
	having sum(amountmst)  <> 0

	DECLARE @createtmpvendtrans_2 NVARCHAR(4000)
	SET @createtmpvendtrans_2 = 'create table ops.vendtrans (
	voucher varchar(20), value numeric(32,16), transdate DATETIME2(6), approveddate DATETIME2(6), invoice varchar(50),
	accountnum varchar(20), dataareaid varchar(3), duedate DATETIME2(6), settledate DATETIME2(6), sub int
	)'
	EXECUTE sp_executesql @createtmpvendtrans_2
	
	

	Insert into ops.vendtrans
	select vt.voucher
		,sum(amountmst) as value,
		convert(date,min(transdate)) as transdate  ,
		convert(date,min(approveddate)) approveddate,
		min(vt.invoice) invoice,
		min(vt.accountnum) accountnum,
		min(vt.dataareaid) dataareaid,
		convert(date,min(duedate)) duedate,
		convert(date,min(lastsettledate)) as settledate --cas 06022020
							  ,1
	from lh_silver_correction.d365.vendtrans vt
	inner join
	lh_silver_correction.d365.generaljournalentry je
		on	je.subledgervoucher = vt.voucher
		and	je.subledgervoucherdataareaid = vt.dataareaid
	inner join 
		lh_silver_correction.d365.generaljournalaccountentry ae
	on
		ae.generaljournalentry = je.recid
	join ops.temp_vendtrans paid on paid.invoice = vt.invoice
							and @company = vt.dataareaid
	where 
		je.subledgervoucherdataareaid = @company
		and convert(date,je.accountingdate) > convert(date,approveddate)
		and (convert(date,lastsettledate) <= @transto or convert(date,approveddate) > @transto)
		and  (convert(date,lastsettledate) >= @transto or convert(date,approveddate) > @transto)
		and vt.voucher not in (select voucher from ops.temp_paid)
		and accountingevent = 0
		and vt.dataareaid not in ('DGJ','DGK')
	group by vt.voucher
	having sum(amountmst) <> 0

	Insert into ops.vendtrans
	select vendtrans.voucher
		,sum(amountmst) as value,
		convert(date,min(transdate)) as transdate  ,
		convert(date,min(approveddate)) approveddate,
		min(vendtrans.invoice) invoice,
		min(vendtrans.accountnum) accountnum,
		min(dataareaid) dataareaid,
		convert(date,min(duedate)) duedate,
		convert(date,min(lastsettledate)) as settledate --cas 06022020
		,2

	from lh_silver_correction.d365.vendtrans --
	join ops.temp_vendtrans paid	on paid.invoice = vendtrans.invoice
	and @company = vendtrans.dataareaid
	where (convert(date,lastsettledate) <= @transto or convert(date,approveddate) >@transto) 
	and accountingevent = 0
	and vendtrans.voucher not in (select voucher from ops.vendtrans vt)
	and vendtrans.voucher not in (select voucher from ops.temp_paid)
	group by vendtrans.voucher
	having sum(amountmst) <> 0

	Insert into ops.vendtrans
	select voucher
		,sum(amountmst) as value,
		convert(date,min(transdate)) as transdate ,
		convert(date,min(approveddate)) approveddate,
		min(vendtrans.invoice) invoice,
		min(vendtrans.accountnum) accountnum,
		min(dataareaid) dataareaid,
		convert(date,min(duedate)) duedate,
		convert(date,min(lastsettledate)) as settledate --cas 06022020
		,3
	from lh_silver_correction.d365.vendtrans --
	where   (convert(date,lastsettledate) >= @transto or convert(date,approveddate) > @transto)
	and transdate <= @transto
	and voucher not in (select voucher from ops.vendtrans vt)
	and voucher not in (select voucher from ops.temp_paid)
	and accountingevent = 0	
	group by voucher
	having sum(amountmst) <> 0

	Insert into ops.vendtrans  
	select voucher
		,sum(amountmst) as value,
		convert(date,min(transdate)) as transdate ,
		convert(date,min(approveddate)) approveddate,
		min(invoice) invoice,
		min(accountnum) accountnum,
		min(dataareaid) dataareaid,
		convert(date,min(duedate)) duedate,
		convert(date,min(lastsettledate)) as settledate --cas 06022020
		,4
	from lh_silver_correction.d365.vendtrans --
	where   convert(date,approveddate) > @transto
	and voucher not in (select voucher from ops.vendtrans vt)
	group by voucher
	having sum(amountmst) <> 0
------------------------------------------------------------------	 

	Select distinct
		lt.dataareaid,
		  lt.voucher as 'Voucher',
		  min(lt.accountdate) as 'Trans Date',
		  max(lt.documentdate) as 'Doc Date',
		  min(vt.approveddate) as 'Approved Date',
		  vt.duedate as 'DueDate',
		  min(ltrim(v.accountnum) + ' - ' + ljt.ledgerdimensionname) as 'Vendor', 
		  min(ltrim(vt.invoice)) as 'Supplier Invoice',
		  sum(lt.amountcur) as 'Amount Excl VAT',
		  min(lt.currencycode) as 'Currency',
		  sum(lt.amountmst) as 'Amount Euro Excl VAT',
		  min(ljt.purchidrange) as 'Purchase order(s)'
	from 
		ops.temp_lt as lt	--was	[bisql].hub.dynamicsv6.ledgertrans as lt
	join ops.vendtrans as vt
			on	lt.voucher = vt.voucher
			and	lt.dataareaid = vt.dataareaid		
	inner join
		lh_silver_correction.d365.vendtable as v
		on	vt.accountnum = v.accountnum
		and	vt.dataareaid = v.dataareaid
	--to get further vendor detail
	left outer join
		lh_silver_correction.d365.dirpartytable dpt3
	on
			dpt3.recid = v.party
		and dpt3.languageid = 'en-gb'
		--and dpt3.instancerelationtype = 2978	
	inner join
		  (select	voucher, ledgerdimensionname,
					max(purchidrange) purchidrange,
					 min(dataareaid) dataareaid
		  from lh_silver_correction.d365.ledgerjournaltrans
		  group by voucher, ledgerdimensionname) as ljt
		on	lt.voucher = ljt.voucher
		and	lt.dataareaid = ljt.dataareaid
	where	
		lt.ledgeraccount like  '%' + @ledgeracc + '%'
	and lt.accountdate <= @transto 
	and (lt.dataareaid = @company or @company = 'all')
	and	left(lt.voucher,2) = 'PZ'
	and vt.duedate <= @dueto

	group by lt.dataareaid 
	,lt.voucher
	,vt.duedate

	having
		 abs(sum(lt.amountmst)) > 0
	order by 1,2

	DROP TABLE IF EXISTS ops.temp_max1
	DROP TABLE IF EXISTS ops.vendtrans
	DROP TABLE IF EXISTS ops.temp_lt
	DROP TABLE IF EXISTS ops.temp_paid 
	DROP TABLE IF EXISTS ops.temp_vendtrans


 END