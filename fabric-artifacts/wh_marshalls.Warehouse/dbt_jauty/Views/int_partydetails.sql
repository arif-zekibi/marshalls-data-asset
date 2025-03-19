-- Auto Generated (Do not modify) 6C6455C0E9A3CCAB61043736A4F4A49D550D66893C5D2A6D8FF026AC5A8C87CB
create view "dbt_jauty"."int_partydetails" as select
    dpt.recid as [partyrecid],
    dpt.languageid as [partylanguage],
    -- , dpt.dataarea			as [legalentitydataareaid]
    dpt. [name] as [partyname],
    dpt.partynumber as [partynumber],
    dpt.instancerelationtype,
    case
        when dpt.instancerelationtype = 3341
        then 'team'
        when dpt.instancerelationtype = 5539
        then 'organisation'
        when dpt.instancerelationtype = 10725
        then 'operating unit'
        when dpt.instancerelationtype = 12506
        then 'person'
        when dpt.instancerelationtype = 20900
        then 'legal entity'
        else 'other'
    end as partytype,
    dpt.primaryaddresslocation,
    dpt.primarycontactemail,
    dpt.primarycontactphone,
    leae.locator as [email],
    leap.locator as [phone],
    leaf.locator as [fax],
    lpa.street,
    lpa.city,
    lpa.county as [countyid],
    lac. [name] as [county],
    lpa. [state] as [stateid],
    las. [name] as [state],
    lpa.zipcode,
    -- , case when len(lpa.countryregionid) = 2 then lpa.countryregionid else
    -- creg.isocode end				as [isocode]
    creg.isocode as [isocode],
    case
        when creg.isocode in ('gb', 'uk')
        then lpa.zipcode
        else creg.isocode + lpa.zipcode
    end as [bi_postcode],
    lpa.countryregionid,
    lacrt_gb.shortname as country,
    lpa. [address],  -- , replace(lpa.address,'%1',lacrt_gb.shortname) as address
    lpa.latitude,
    lpa.longitude,
    lacrt_native.shortname as country_native,
    replace(lpa.address, '%1', lacrt_native.shortname) as address_native,
    namealias,
    dpt. [partition] as [partition],
    null as [orgnumber]  -- dpt.orgnumber

from "lh_silver_correction"."d365"."dirpartytable"  dpt
left join
    "lh_silver_correction"."d365"."logisticspostaladdress" lpa
    on dpt.primaryaddresslocation = lpa. [location]
    and dpt. [partition] = lpa. [partition]
    and getdate() > lpa.validfrom
    and getdate() < lpa.validto
left join
    "lh_silver_correction"."d365"."logisticselectronicaddress" leae
    on dpt.primarycontactemail = leae.recid
    and dpt. [partition] = leae. [partition]
left join
    "lh_silver_correction"."d365"."logisticselectronicaddress" leap
    on dpt.primarycontactphone = leap.recid
    and dpt. [partition] = leap. [partition]

left join
    "lh_silver_correction"."d365"."logisticselectronicaddress" leaf
    on dpt.primarycontactfax = leaf.recid
    and dpt. [partition] = leaf. [partition]
left join
    "lh_silver_correction"."d365"."logisticsaddresscountryregion" creg
    on lpa.countryregionid = creg.countryregionid
left join
    "lh_silver_correction"."d365"."logisticsaddresscountryregiontranslation" lacrt_native
    on lpa.countryregionid = lacrt_native.countryregionid
    and dpt.languageid = lacrt_native.languageid
    and lpa. [partition] = lacrt_native. [partition]
left join
    "lh_silver_correction"."d365"."logisticsaddresscountryregiontranslation" lacrt_gb
    on lpa.countryregionid = lacrt_gb.countryregionid
    and 'en-gb' = lacrt_gb.languageid
    and lpa. [partition] = lacrt_gb. [partition]
left join
    "lh_silver_correction"."d365"."logisticsaddresscounty" lac  -- duplcate
    on lpa.county = lac.countyid
    and lpa.countryregionid = lac.countryregionid
    and lpa. [state] = lac.stateid
left join
    "lh_silver_correction"."d365"."logisticsaddressstate" las
    on lpa. [state] = las.stateid
    and lpa.countryregionid = las.countryregionid
    and lpa. [partition] = las. [partition];