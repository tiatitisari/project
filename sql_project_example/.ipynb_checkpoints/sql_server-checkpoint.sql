--PORTOMIX--

drop table #TENT
go

select
a.DATE_TIME,a.WILAYAH_BARU,a.WILAYAH,a.AREA,a.KD_CAB,a.NM_CAB,a.NO_CUS,a.NO_REK,a.NM_CUS,a.JNS_KRD,a.BUC_REPORTING,
a.BIKOLE,a.BMKOLE as CLASCD,a.ORGAMT,a.LIMIT,a.CBALRP_NET_PSAK,a.CURTYP,a.KURS,a.RATE,a.ACCSTS,a.BILPRN,a.BILINT,
a.MATDAT,a.PURCOD, a.BISEKT8 as BISEKT,
a.PRNDUE,a.INTDUE,
DPD = 
case
when isnull(a.PRNDUE,0) >= isnull(a.INTDUE,0) then isnull(a.PRNDUE,0)
when isnull(a.PRNDUE,0) < isnull(a.INTDUE,0) then isnull(a.INTDUE,0)
else null
end,
a.RESTRU,a.FCODE, a.REVOLV_FG as REVOLV,
a.FRELDT,a.STSDT,a.MTDINTrp,a.MTDPRNrp,a.MTDLCrp,a.MTDOTHrp,a.BISIFA

into #TENT

from 
krdGAB.dbo.KRD_FINAL_202007 a
where a.BUC_REPORTING like 'SM%'
go



---JOIN LAST MONTH----
drop table #JOIN_TENT
go

select 
a.*, 
B.date_time as dt, 
B.newORGAMT2016 as newORGAMT2016_LM,
B.CBALRP_NET_PSAK as CBALRP_NET_PSAK_LM,
(isnull(a.CBALRP_NET_PSAK,0) - isnull(b.CBALRP_NET_PSAK,0)) as DeltaBade
into #JOIN_TENT
from #TENT a
left join dailysmall.dbo.GABSMALL_202006 b on a.no_rek = b.no_rek and datediff(month,a.date_time,b.date_time) = -1
where a.date_time = '2020-07-31'
go 



alter table #JOIN_TENT add newORGAMT2016 decimal null
go

update #JOIN_TENT
set newORGAMT2016 = null
go

update #JOIN_TENT
set newORGAMT2016 = ORGAMT * KURS
where date_time =  '2020-07-31' 
go

select * from #JOIN_TENT where newORGAMT2016 is null
go

update #JOIN_TENT
set newORGAMT2016 = b.newORGAMT2016
from #JOIN_TENT a inner join dailysmall.dbo.GABSMALL_201903 b on a.no_rek = b.no_rek and a.date_time = b.date_time
where a.date_time =  '2020-07-31' and a.newORGAMT2016 is null
go

drop table #newORGAMTbyNOcus2016
go
select date_time,NO_CUS,sum(newORGAMT2016) as ORGAMTbyNOcus2016
into #newORGAMTbyNOcus2016
from #JOIN_TENT
where date_time =  '2020-07-31'
group by date_time,NO_CUS
go

alter table #JOIN_TENT add ORGAMTbyNOcus2016 decimal null
go

update #JOIN_TENT
set ORGAMTbyNOcus2016 = b.ORGAMTbyNOcus2016
from #JOIN_TENT a inner join #newORGAMTbyNOcus2016 b on a.NO_CUS = b.NO_CUS and a.date_time = b.date_time
where a.date_time =  '2020-07-31'
go


alter table #JOIN_TENT add bucketLimitORGAMTbyNOcus201704 nvarchar(50) null
go

update #JOIN_TENT
set bucketLimitORGAMTbyNOcus201704 =
case
when ORGAMTbyNOcus2016 <= 500000000 then '1.s.d.RP.500jt'
when ORGAMTbyNOcus2016 > 500000000 and ORGAMTbyNOcus2016 <= 1000000000 then '2.>Rp.500jt � 1M'
when ORGAMTbyNOcus2016 > 1000000000 and ORGAMTbyNOcus2016 <= 2000000000 then '3.>Rp.1M � 2M'
when ORGAMTbyNOcus2016 > 2000000000 and ORGAMTbyNOcus2016 <= 5000000000 then '4.>Rp.2M � 5M'
when ORGAMTbyNOcus2016 > 5000000000 and ORGAMTbyNOcus2016 <= 10000000000 then '5.>Rp.5M � 10M'
when ORGAMTbyNOcus2016 > 10000000000 then '6.>Rp.10M'
else null
end
where date_time =  '2020-07-31'
go

alter table #JOIN_TENT add bucketLimitORGAMTbyNOcus2M_2016 nvarchar(50) null
go
-------------------------------------------------------------------------------
update #JOIN_TENT
set bucketLimitORGAMTbyNOcus2M_2016 =
case
when ORGAMTbyNOcus2016 <= 2000000000 then '1.<= 2M'
when ORGAMTbyNOcus2016 >  2000000000 then '2. > 2M'
else null
end
where date_time =  '2020-07-31'
go


alter table #JOIN_TENT add bucket nvarchar(20) null
go

update #JOIN_TENT
set
bucket = 
case 
when isnull(DPD,0) = 0 then 'A. lancar'
when DPD between 1   and 30  then 'B. 1-30'
when DPD between 31  and 60  then 'C. 31-60'
when DPD between 61  and 90  then 'D. 61-90'
when DPD between 91  and 120 then 'E. 91-120'
when DPD between 121 and 150 then 'F. 121-150'
when DPD between 151 and 180 then 'G. 151-180'
when DPD between 181 and 210 then 'H. 181-210'
when DPD between 211 and 240 then 'I. 211-240'
when DPD between 241 and 270 then 'J. 241-270'
when DPD >= 271  then 'K. > 270'
end
where date_time =  '2020-07-31'
go


update #JOIN_TENT
set 
CLASCD =
case
when BIKOLE in ('1','01') then '01'
when BIKOLE in ('2','02') then '2A'
when BIKOLE in ('3','03') then '03'
when BIKOLE in ('4','04') then '04'
when BIKOLE in ('5','05') then '05'
else CLASCD
end
where CLASCD is null or CLASCD = ''
go




alter table #JOIN_TENT add produk4 nvarchar(20) null
go

update #JOIN_TENT
set produk4= NULL
GO

update #JOIN_TENT
set produk4='KI'
where JNS_KRD LIKE 'I-%' or (JNS_KRD LIKE '%FLEX%' AND PURCOD IN ('SM026', '75','79'))
go

update #JOIN_TENT
set produk4='KMK'
where JNS_KRD LIKE '%KMK%' or JNS_KRD LIKE 'M%' or JNS_KRD LIKE 'W01' or (JNS_KRD LIKE '%FLEX%' AND PURCOD NOT IN ('SM026', '75','79')) 
go

update #JOIN_TENT
set produk4='KAD'
where JNS_KRD like '%DANA%' or JNS_KRD like '%CASH%' ---or JNS_KRD like '%GIRO%' 
go


---------------------------
   ----BARU  20170802---
---------------------------
update #JOIN_TENT
set produk4='KMK'
where JNS_KRD like '%GIRO%' 
go
---------------------------


update #JOIN_TENT set produk4='NON-CASHLOAN' where JNS_KRD IN ('TR','PD','FORFAITING') and produk4 is null
go

update #JOIN_TENT
set produk4='NON-CASHLOAN' 
where JNS_KRD IN ('OD','OD-INT-IDR','DF','ODP (USD)','OD-INT-USD','OD-0 INT') and produk4 is null
go

update #JOIN_TENT
set produk4='KI'
where JNS_KRD IN ('KI-MKUKBB','P-KOP-AN','P-KOP-FL','S-KHUSUSA') and produk4 is null
go




----------------
-----BOOKING----
----------------

------------------
------------------

---BOOKING 20170802---


alter table #JOIN_TENT add BOOKING nvarchar(50) null
go


-----------------------------
----------New Debtor-------
-----------------------------
CIF <> LM ---AND CIF <> LY 
FRELDATE = DATE TIME OR FRELDATE is null ORGDATE  = DATE TIME
BIKOLE =1

exclude :> 
M-POSTFIN
M-POSTNCL
GIRODBT
jns_krd NOT LIKE '%PD%' --PD
jns_krd NOT LIKE '%NCL%'


update #JOIN_TENT set BOOKING = null
go


update #JOIN_TENT
set BOOKING = 'DEBITUR BARU'
from #JOIN_TENT 
where  
NO_CUS not in (select distinct NO_CUS from ccaBB.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA
) and
----NO_CUS not in (select distinct NO_CUS from dailySMALL.dbo.GABSMALL_201612) and (LAST YEAR) --> take OUT 201708
(convert(varchar(7),FRELDT,20) = convert(varchar(7),DATE_TIME,20) OR FRELDT is null) and
BIKOLE in ('01','1') and
JNS_KRD not in ('M-POSTFIN','M-POSTNCL','GIRODBT','FORFAITING') and
jns_krd NOT LIKE '%PD%' and--PD
jns_krd NOT LIKE '%NCL%'
and BOOKING is null
go
---680



select BOOKING,count(1) from #JOIN_TENT group by BOOKING order by BOOKING

-----------------------------------------------------
---TopUp New Account---
-----------------------------------------------------

update #JOIN_TENT
set BOOKING = 'TOP UP REK BARU CIF LAMA KOP - MULTIACCT'
where 
NO_CUS in (select distinct NO_CUS from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA 
) and 
NO_REK not in (select distinct NO_REK from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA
) and
(JNS_KRD in ('M-MULACCT','P-KMKM-AN','P-KMKM-DL') OR JNS_KRD like '%KOP%') and
convert(varchar(7),FRELDT,20) = convert(varchar(7),DATE_TIME,20) and
BIKOLE in ('01','1') 
and BOOKING is null 
go




> KAD & TR 
-------------

- CIFnya ada di bulan lalu
- NOREK tdk ada di bulan lalu
- JNS_KRD in %DANA%, %CASH%, TR, DF, OD%
- FRELDATE = DATE TIME  atw FRELDATE is null---(TR FRELDATE NULL abaikan)
- BIKOLE =1

update #JOIN_TENT
set BOOKING = 'TOP UP REK BARU CIF LAMA KAD - TR'
where 
NO_CUS in (select distinct NO_CUS from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA
) and 
NO_REK not in (select distinct NO_REK from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA
) and
(JNS_KRD like '%DANA%'  or JNS_KRD like '%CASH%' or ltrim(rtrim(JNS_KRD)) = 'TR' or ltrim(rtrim(JNS_KRD)) = 'DF'
or JNS_KRD like 'OD%') and
(convert(varchar(7),FRELDT,20) = convert(varchar(7),DATE_TIME,20) or FRELDT is null) and
BIKOLE in ('01','1')
and BOOKING is null
go



> UMUM (others)
----------------
update #JOIN_TENT
set BOOKING = 'TOP UP REK BARU UMUM'
from #JOIN_TENT a
left join 
(select distinct date_time,NO_CUS,ORGAMTbyNOcus2016 from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA
) b on a.NO_CUS = b.NO_CUS
where  
a.ORGAMTbyNOcus2016 > b.ORGAMTbyNOcus2016 and
a.NO_REK not in (select distinct NO_REK from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA 
) and
convert(varchar(7),a.FRELDT,20) = convert(varchar(7),a.DATE_TIME,20) and
a.BIKOLE in ('01','1') and
a.JNS_KRD not in ('M-POSTFIN','M-POSTNCL','GIRODBT','FORFAITING')
and a.BOOKING is null
go




> NEW TOP UP untuk KAD & TR
----------------------------
update #JOIN_TENT
set BOOKING = 'NEW TOP UP untuk KAD-TR CIF tdk ada dLM ad di LY'
where 
NO_CUS in (select distinct NO_CUS from dailySMALL.dbo.GABSMALL_201912) and 
NO_CUS not in (select distinct NO_CUS from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA 
) and
NO_REK not in (select distinct NO_REK from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA
) and
(
(JNS_KRD like '%DANA%'  or JNS_KRD like '%CASH%' and convert(varchar(7),FRELDT,20) = convert(varchar(7),DATE_TIME,20)) OR
((ltrim(rtrim(JNS_KRD)) = 'TR' or ltrim(rtrim(JNS_KRD)) = 'DF')  and (convert(varchar(7),FRELDT,20) = convert(varchar(7),DATE_TIME,20) or FRELDT is null ))
) and
BIKOLE in ('01','1') and
BOOKING is null
go



---------------------------------------------------------------------
              ---Top UP -----
---------------------------------------------------------------------

di luar kondisi yang atas :>

LIMIT CIF BULAN POSISI > BULAN SEBELUMNYA
LIMIT NO_REK > BULAN SEBELUMNYA
NOREK = LM

exclude :> 
M-POSTFIN
M-POSTNCL
GIRODBT

BIKOLE = 1


update #JOIN_TENT
set BOOKING = 'TOP UP REK & CIF SDH ADA SEBELUMNYA'
from #JOIN_TENT a
left join ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA
b on a.NO_REK = b.NO_REK
where  
a.ORGAMTbyNOcus2016 > b.ORGAMTbyNOcus2016 and
a.newORGAMT2016 > b.newORGAMT2016 and
a.CBALRP_NET_PSAK > b.CBALRP_NET_PSAK and ---20170523
a.NO_REK in (select distinct NO_REK from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA --dailySMALL.dbo.GABSMALL_201903
) and
a.BIKOLE in ('01','1') and
a.JNS_KRD not in ('M-POSTFIN','M-POSTNCL','GIRODBT','FORFAITING')
and a.BOOKING is null
---20170523
and a.DELTABADE >= 0
go




------------------------
-----Investment Credit-------
------------------------

----BARU----

update #JOIN_TENT
set BOOKING = 'PENARIKAN KI'
from #JOIN_TENT 
where  
NO_REK in (select distinct NO_REK from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA 
) and
DeltaBade >= 0 and 
--(ltrim(rtrim(REVOLV)) = 'N' 
(ltrim(rtrim(REVOLV)) = 'N' 
or jns_krd LIKE 'I-%') and
JNS_KRD not like '%DANA%' and JNS_KRD not like '%CASH%' and
JNS_KRD not in ('M-POSTFIN','M-POSTNCL','GIRODBT','M-KUK','M-KUKAN','M-KUK-NB','W-KMK') and
jns_krd NOT LIKE '%PD%'  and --PD
jns_krd NOT LIKE '%NCL%' and
jns_krd NOT LIKE 'OD%' and ---new 20170802  
BIKOLE in ('01','1') 
and BOOKING is null
go



----MULTIACCOUNT---

update #JOIN_TENT
set BOOKING = 'PENCAIRAN KAD-MULTIACCT NR'
from #JOIN_TENT 
where 
convert(varchar(7),FRELDT,20) = convert(varchar(7),DATE_TIME,20) and
JNS_KRD not in ('M-POSTFIN','M-POSTNCL','GIRODBT') and
jns_krd NOT LIKE '%PD%' and--PD
jns_krd NOT LIKE '%NCL%' and BIKOLE in ('01','1') and
(JNS_KRD = 'M-MULACCT' or JNS_KRD like '%DANA%'  or JNS_KRD like '%CASH%') and
ltrim(rtrim(REVOLV)) = 'N' 
and BOOKING is null
go

select distinct BOOKING from #JOIN_TENT

alter table #JOIN_TENT add BADE_BOOKING float null
go

update #JOIN_TENT
set BADE_BOOKING = null
where date_time = '2018-10-31'
go



update #JOIN_TENT
set
BADE_BOOKING = 
case
when BOOKING in ('PENARIKAN KI','TOP UP REK & CIF SDH ADA SEBELUMNYA')
then DELTABADE else CBALRP_NET_PSAK end
go




select * from #JOIN_TENT
where BOOKING in ('PENARIKAN KI','TOP UP REK & CIF SDH ADA SEBELUMNYA') and DELTABADE < 0
 


select * from #JOIN_TENT

select BOOKING,sum(CBALRP_NET_PSAK) as CBALRP_NET_PSAK,sum(BADE_BOOKING) as BADE_BOOKING,count(1) from #JOIN_TENT group by BOOKING order by BOOKING



select distinct substring(no_rek,1,3) from #JOIN_TENT order by substring(no_rek,1,3)


select  
ACCTNO = no_rek,*
into ccabb.dbo.DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
from #JOIN_TENT
go



alter table DATA_BB_20200731_EDM_FINAL_NEWKRITERIA add bucketLimitORGAMTbyNOcus201704 nvarchar(50) null
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set bucketLimitORGAMTbyNOcus201704 =
case
when ORGAMTbyNOcus2016 <= 500000000 then '1.s.d.RP.500jt'
when ORGAMTbyNOcus2016 > 500000000 and ORGAMTbyNOcus2016 <= 1000000000 then '2.>Rp.500jt � 1M'
when ORGAMTbyNOcus2016 > 1000000000 and ORGAMTbyNOcus2016 <= 2000000000 then '3.>Rp.1M � 2M'
when ORGAMTbyNOcus2016 > 2000000000 and ORGAMTbyNOcus2016 <= 5000000000 then '4.>Rp.2M � 5M'
when ORGAMTbyNOcus2016 > 5000000000 and ORGAMTbyNOcus2016 <= 10000000000 then '5.>Rp.5M � 10M'
when ORGAMTbyNOcus2016 > 10000000000 then '6.>Rp.10M'
else null
end
where date_time = '2020-07-31'
go

alter table DATA_BB_20200731_EDM_FINAL_NEWKRITERIA add bucketLimitORGAMTbyNOcus2M_2016 nvarchar(50) null
go
-------------------------------------------------------------------------------
update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set bucketLimitORGAMTbyNOcus2M_2016 =
case
when ORGAMTbyNOcus2016 <= 2000000000 then '1.<= 2M'
when ORGAMTbyNOcus2016 >  2000000000 then '2. > 2M'
else null
end
where date_time = '2020-07-31'
go



------------------------------------
--------------UPDATE MOB------------
------------------------------------
alter table DATA_BB_20200731_EDM_FINAL_NEWKRITERIA add newFRELDATE datetime null 
go

----------------------
----UPDATE MOB-----
----------------------

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set newFRELDATE =  null
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
newFRELDATE = 
case
when BOOKING is not null then date_time
else freldt
end
where date_time = '2020-07-31'  and newFRELDATE is null
go


alter table portomix2020add CForgDate datetime null go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA 
set 
CForgDate = b.CForgDate
--,mobCairByCIFopen = datediff(month,b.CForgDate,a.FRELDT)
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a inner join ccaBB.dbo.CFMAST ---CustomerNumberDate 
b on a.no_cus = b.CustomerNumber and a.date_time = '2020-07-31'
go
----45994 40092

alter table DATA_BB_20200731_EDM_FINAL_NEWKRITERIA add NEW_MOBCIF float null
go

---YG DIPAKAI--
update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set
NEW_MOBCIF = datediff(month,CForgDATE,FRELDT)
go

select * from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
go


---------------------
---------------------
---------------------
--------TOP UP-------
---------------------
---------------------
---------------------

-----------
---START---
-----------


drop table #JOIN_TENT_TOPUP
go
select A.*, 
b.DATE_TIME as DATE_TIME_LM, b.NO_CUS as NO_CUS_LM,  b.ORGAMTbyNOcus2016 as ORGAMTbyNOcus2016_LM, b.FRELDT as FRELDT_LM,
flag = 
case
when isnull(a.ORGAMTbyNOcus2016,0) > isnull(b.ORGAMTbyNOcus2016,0) and b.no_cus is not null then 'topUp'
else null
end,
datediff(month,b.FRELDT,a.DATE_TIME) as BulanKe,

case
when datediff(month,b.FRELDT,a.DATE_TIME) <= 6 then '1. <=6Bln'
when datediff(month,b.FRELDT,a.DATE_TIME) > 6 and datediff(month,b.FRELDT,a.DATE_TIME) <= 12 then '2. >6 sd 12Bln'
when datediff(month,b.FRELDT,a.DATE_TIME) > 12 and datediff(month,b.FRELDT,a.DATE_TIME) <= 24 then '3. >12 sd 24Bln'
when datediff(month,b.FRELDT,a.DATE_TIME) > 24 then '4. >24Bln'
else null
end as bucketBulanKe

into #JOIN_TENT_TOPUP
from
(select DATE_TIME, NO_CUS, ORGAMTbyNOcus2016, max(FRELDT) as FRELDT
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA ---#JOIN_TENT 
group by DATE_TIME, NO_CUS, ORGAMTbyNOcus2016) A
left join
(select DATE_TIME, NO_CUS, ORGAMTbyNOcus2016, max(FRELDT) as FRELDT
from ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA --GABSMALL_201712 
group by DATE_TIME, NO_CUS, ORGAMTbyNOcus2016) B on cast(a.NO_CUS as float) = cast(b.NO_CUS as float)
go


drop table #JOIN_TENT2
go
select a.DATE_TIME,a.NO_CUS ,a.NO_REK, a.FRELDT , a.newORGAMT2016, b.newORGAMT2016 as newORGAMT2016_LM
into #JOIN_TENT2
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a---#JOIN_TENT a
left join ccabb.dbo.DATA_BB_20200630_EDM_FINAL_NEWKRITERIA b on a.NO_REK = b.NO_REK --cast(a.NO_REK as float) = cast(b.NO_REK as float)
where a.NO_CUS in (select no_cus from #JOIN_TENT_TOPUP where flag is not null)
order by a.NO_CUS
go
---4401 4625



drop table ##JOIN_TENT3
go
select a.*, b.NO_REK, b.FRELDT as FRELDT_norek, b.newORGAMT2016,b.newORGAMT2016_LM,
flagRekTopUpCIF = 
case
when 
isnull(b.newORGAMT2016,0) > isnull(b.newORGAMT2016_LM,0)  then 1
else null
end
into ##JOIN_TENT3
from #JOIN_TENT_TOPUP a
left join #JOIN_TENT2 b on a.no_cus = b.no_cus
where a.flag is not null
go
---4401 4625

---------------
select date_time,count(1)
from ##JOIN_TENT3
group by date_time
order by date_time
go

select date_time,count(1)
from ccaBB.dbo.TB_LIST_TOPUPCIFBB
group by date_time
order by date_time desc 
go



insert into ccaBB.dbo.TB_LIST_TOPUPCIFBB
(DATE_TIME,NO_CUS,ORGAMTbyNOcus2016,FRELDT,DATE_TIME_LM,NO_CUS_LM,ORGAMTbyNOcus2016_LM,FRELDT_LM,flag,BulanKe,bucketBulanKe,NO_REK,
FRELDT_norek,newORGAMT2016,newORGAMT2016_LM,flagRekTopUpCIF)

select 
convert(datetime,convert(varchar(10),DATE_TIME,20)) as DATE_TIME,
NO_CUS,ORGAMTbyNOcus2016,FRELDT,DATE_TIME_LM,NO_CUS_LM,ORGAMTbyNOcus2016_LM,FRELDT_LM,flag,BulanKe,bucketBulanKe,NO_REK,
FRELDT_norek,newORGAMT2016,newORGAMT2016_LM,flagRekTopUpCIF
from ##JOIN_TENT3
go


alter table ccaBB.dbo.TB_LIST_TOPUPCIFBB add ketTopUp_NEW nvarchar(50)null



----new cek 20170315
update ccabb.dbo.TB_LIST_TOPUPCIFBB set 
ketTopUp_NEW =
case
when isnull(newORGAMT2016,0) > isnull(newORGAMT2016_LM,0) and newORGAMT2016_LM is not null then 'TopUpRek'
when isnull(newORGAMT2016,0) > isnull(newORGAMT2016_LM,0) and newORGAMT2016_LM is null then 'TopUpCIF'
else null
end
where date_time = '2020-07-31'
go



alter table ccabb.dbo.TB_LIST_TOPUPCIFBB add freldt_TopUp datetime null
go



update ccabb.dbo.TB_LIST_TOPUPCIFBB
set 
freldt_TopUp = case when ketTopUp_NEW is not null then date_time else null end
where date_time = '2020-07-31'
go



update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set  ketTopUp_NEW_pos = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from ccaBB.dbo.TB_LIST_TOPUPCIFBB_20160823 ) b 
on a.no_rek = b.no_rek and  convert(varchar(7),a.date_time,20) = convert(varchar(7),b.date_time,20)
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set  

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-01') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-02') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-03') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-04') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-05') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-06') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-07') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-08') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-09') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-10') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-11') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2014-12') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-01') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-02') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-03') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-04') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-05') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-06') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-07') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-08') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-09') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-10') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-11') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2015-12') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-01') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-02') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-03') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-04') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-05') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-06') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-07') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-08') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-09') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-10') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-11') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2016-12') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-01') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-02') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-03') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-04') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-05') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-06') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-07') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-08') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-09') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-10') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-11') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2017-12') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-01') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-03') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-03') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-04') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-06') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-06') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-07') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-08') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-09') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-10') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-11') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2018-12') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-01') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-02') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-03') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-04') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-05') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-06') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-07') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-08') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-09') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-10') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-11') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2019-12') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2020-01') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2020-02') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2020-03') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2020-04') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2020-05') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2020-06') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 

freldt_TopUp = b.freldt_TopUp, ketTopUp_NEW = b.ketTopUp_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join  (select * from TB_LIST_TOPUPCIFBB where convert(varchar(7),date_time,20) = '2020-07') b on a.no_rek = b.no_rek and  a.date_time >= b.date_time 
where convert(varchar(7),a.date_time,20) in('2020-07')
go



update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set newFRELDATE =  null
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
newFRELDATE = 
case
when BOOKING is not null then date_time
else freldt
end
where date_time = '2020-07-31'  and newFRELDATE is null
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set
freldt_Final = null
go

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set
freldt_Final =
case
when BOOKING is not null then date_time
when BOOKING is null and freldt_TopUp is not null then newFRELDATE
when freldt_TopUp is not null then freldt_TopUp
else newFRELDATE
end
where date_time = '2020-07-31'
--where date_time >= '2017-01-01'
go


select * from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
where BOOKING is null and  datediff(month,FRELDT_FINAL, date_time) <> 0
go

select * from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
where BOOKING is not null and  datediff(month,FRELDT_FINAL, date_time) <> 0
go

select * from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
where BOOKING is null and  datediff(month,FRELDT_FINAL, date_time) = 0

select * from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
where BOOKING is not null and  datediff(month,FRELDT_FINAL, date_time) = 0

select no_cus,no_rek,FRELDT_FINAL,newmob =datediff(month,FRELDT_FINAL, date_time)
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
order by no_cus,no_rek


select * from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
where freldt_Final is not null

--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------


----------------------
------PORTOFOLIO------
----------------------

alter table DATA_BB_20200731_EDM_FINAL_NEWKRITERIA add 
NEW_PRIMA_KRITERIA_201708 nvarchar(255) null,
NEW_VC_KRITERIA_201708 nvarchar(255) null,
NEW_KOPERASI_KRITERIA_201708 nvarchar(255) null
go


--------
---VC---
--------


---START VC---
drop table #minFRELDT
go

select DATE_TIME,NO_CUS,min(FRELDT) as FRELDT
into #minFRELDT
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
group by DATE_TIME,NO_CUS
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_VC_KRITERIA_201708 = c.VC
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a
inner join #minFRELDT b on a.NO_CUS = b.NO_CUS and convert(varchar(7),a.date_time,20) = convert(varchar(7),b.date_time,20)
inner join LISTVALUECHAIN c on a.NO_CUS = c.LIST_CIF ---and convert(varchar(7),c.date_time_list,20) = '2018-07'
where (b.FRELDT is null or convert(varchar(7),b.FRELDT ,20) >= '2010-01')
and a.date_time =  '2020-07-31'
go



-----------------------------------
------------VC LAST MONTH----------
-----------------------------------

drop table #VC_LM
go

select distinct no_rek ,NEW_VC_KRITERIA_201708 as VC
into #VC_LM
from DATA_BB_20200630_EDM_FINAL_NEWKRITERIA 
where convert(varchar(7),date_time,20) = '2020-06' and NEW_VC_KRITERIA_201708 is not null
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_VC_KRITERIA_201708 = b.VC
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a inner join #VC_LM b on a.no_rek = b.no_rek 
where convert(varchar(7),a.date_time,20) = '2020-07' and a.NEW_VC_KRITERIA_201708 is null
go


select date_time,count(1)
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
where NEW_VC_KRITERIA_201708 is not null
group by date_time
order by date_time
go



--------------
---KOPERASI---
--------------
update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_KOPERASI_KRITERIA_201708 = b.SOURCE_PRODUCT_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a inner join LISTKOPERASI b on 
--a.no_rek = b.no_rek 
a.no_rek = b.no_rek --and convert(varchar(7),b.[month],20) = '2018-07'--convert(varchar(7),a.date_time,20) = convert(varchar(7),b.[month],20)
where convert(varchar(7),a.date_time,20) = '2020-07' and a.NEW_KOPERASI_KRITERIA_201708 is null
go
---3683


----by CIF NEW cair---
update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_KOPERASI_KRITERIA_201708 = b.SOURCE_PRODUCT_NEW
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a inner join LISTKOPERASI b on 
--a.no_rek = b.no_rek 
--cast(a.NO_CUS as float) = cast(b.NO_CIF as float) 
ltrim(rtrim(a.NO_CUS)) = ltrim(rtrim(b.NO_CIF))

and convert(varchar(7),b.[month],20) = '2018-07'--convert(varchar(7),a.date_time,20) = convert(varchar(7),b.[month],20)
where convert(varchar(7),a.date_time,20) = '2020-07' and a.NEW_KOPERASI_KRITERIA_201708 is null 
and convert(varchar(7),a.FRELDT,20) = '2020-07'
go
---135


--------------
  ---PRIMA---
--------------

--------------------
-----PRIMA BIASA----
--------------------


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_PRIMA_KRITERIA_201708 = b.KET_PRIMA
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a inner join LISTPRIMA b on 
a.no_rek = b.no_rek ---and convert(varchar(7),b.[date_time],20) = '2018-07'
where convert(varchar(7),a.date_time,20) = '2020-07' and a.NEW_PRIMA_KRITERIA_201708 is null
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_PRIMA_KRITERIA_201708 = b.KET_PRIMA
from DATA_BB_20200731_EDM_FINAL_NEWKRITERIA a inner join LISTPRIMA b on 
a.NO_CUS = b.NO_CUS --and convert(varchar(7),b.[date_time],20) = '2018-07'
where convert(varchar(7),a.date_time,20) = '2020-07' and a.NEW_PRIMA_KRITERIA_201708 is null
and convert(varchar(7),a.FRELDT,20) = '2020-07'
go



--------------------
-----PRIMA TOPUP----
--------------------



---rename column--
sp_RENAME 'DATA_BB_20200731_EDM_FINAL_NEWKRITERIA.BOOKING' , 'BOOKING_NEWKRITERIA201708', 'COLUMN'


---2017-09--
drop table #PRIMA_TOPUP_LM
go

select distinct no_rek 
into #PRIMA_TOPUP_LM
from DATA_BB_20200630_EDM_FINAL_NEWKRITERIA
where convert(varchar(7),date_time,20) IN ('2020-06') 
and NEW_PRIMA_KRITERIA_201708 IN('PRIMA topUP','PRIMA topUP LM')
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_PRIMA_KRITERIA_201708 = 'PRIMA topUP LM'
where convert(varchar(7),date_time,20) = '2020-07' and NEW_PRIMA_KRITERIA_201708 is null and 
no_rek in (select distinct no_rek from #PRIMA_TOPUP_LM)
go




---------------------------------------------
		---NEW TOP UP--
---------------------------------------------

----LMONTH--

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_PRIMA_KRITERIA_201708 = 'PRIMA topUP'
where convert(varchar(7),date_time,20) = '2020-07' and NEW_PRIMA_KRITERIA_201708 is null and 
(
--no_cus in (select distinct no_cus from topupBB.dbo.LIST_POTENSITOPUPBB20171011_FIN_mbDewi) or
no_cus in (select distinct no_cus from dbo.LIST_POTENSITOPUPBB)
)
and BOOKING_NEWKRITERIA201708 like '%TOP%UP%'
go




--------------------------------------
-----PRIMA TB_PRIMA_RELATED_201708----
--------------------------------------


---2017-08--
DROP TABLE #related_lm
GO

SELECT DISTINCT NO_REK 
INTO #related_lm
FROM DATA_BB_20200630_EDM_FINAL_NEWKRITERIA 
WHERE convert(varchar(7),date_time,20) IN ('2020-06')
AND NEW_PRIMA_KRITERIA_201708 = 'PRIMA RELATED'
GO

update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_PRIMA_KRITERIA_201708 = 'PRIMA RELATED'
WHERE convert(varchar(7),date_time,20) = '2020-07' AND NEW_PRIMA_KRITERIA_201708 is null
AND NO_REK IN (SELECT DISTINCT NO_REK FROM #related_lm)
GO


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_PRIMA_KRITERIA_201708 = 'PRIMA RELATED'
WHERE convert(varchar(7),date_time,20) = '2020-07' AND NEW_PRIMA_KRITERIA_201708 is null
AND (BOOKING_NEWKRITERIA201708 LIKE '%DEBITUR%BARU%' OR BOOKING_NEWKRITERIA201708 LIKE '%TOP%UP%')
AND NO_CUS IN (SELECT DISTINCT CU_CIF FROM LISTPRIMARELATEDUPDATE)
GO


-----------------------------------------------------------------
----PRIMA CLUSTER (TAKE OVER), sumber purpose code SM071---
-----------------------------------------------------------------
update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_PRIMA_KRITERIA_201708 = 'PRIMA CLUSTER TAKE OVER'
WHERE convert(varchar(7),date_time,20) = '2020-07' AND NEW_PRIMA_KRITERIA_201708 is null
AND ltrim(rtrim(PURCOD)) = 'SM071'
GO



update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set 
NEW_PRIMA_KRITERIA_201708 = 'PRIMA CLUSTER TAKE OVER'
WHERE convert(varchar(7),date_time,20) = '2020-07' AND NEW_PRIMA_KRITERIA_201708 is null
AND  NO_CUS IN (SELECT DISTINCT [CIFNO] FROM LISTPRIMACLUSTER)
GO


------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------


------------MIX----------

alter table DATA_BB_20200731_EDM_FINAL_NEWKRITERIA 
add NEW_PORTOMIX_NEWKRITERIA201708 nvarchar(255) null
go


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set
NEW_PORTOMIX_NEWKRITERIA201708 = --NEW_PORTOMIX = 
case
WHEN NEW_PRIMA_KRITERIA_201708 IS NOT NULL THEN NEW_PRIMA_KRITERIA_201708
WHEN NEW_VC_KRITERIA_201708 IS NOT NULL THEN NEW_VC_KRITERIA_201708
WHEN NEW_KOPERASI_KRITERIA_201708 IS NOT NULL AND NEW_KOPERASI_KRITERIA_201708 not like '%NON%KOPKAR%'
AND ltrim(rtrim(NEW_KOPERASI_KRITERIA_201708)) <> 'KOPERASI NON KARYAWAN'
THEN NEW_KOPERASI_KRITERIA_201708
ELSE NULL
END
WHERE NEW_PORTOMIX_NEWKRITERIA201708 is null and YEAR(DATE_TIME) = '2020' 
GO


update DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
set
NEW_PORTOMIX_NEWKRITERIA201708 = -- =
case
when NEW_PORTOMIX_NEWKRITERIA201708 is null and isnull(NEW_MOBCIF,0) <= 3 then '05. Others - NTB'
when NEW_PORTOMIX_NEWKRITERIA201708 is null and isnull(NEW_MOBCIF,0) >  3 then '06. Others - ETB'
else NEW_PORTOMIX_NEWKRITERIA201708
end
WHERE NEW_PORTOMIX_NEWKRITERIA201708 is null and YEAR(DATE_TIME) = '2020' --and date_time = '2017-08-31' 
go


-----------
----OUT----
-----------

---rename column--
sp_RENAME 'DATA_BB_20200731_EDM_FINAL_NEWKRITERIA.BADE_BOOKING' , 'BADE_BOOKING_NEWKRITERIA201708', 'COLUMN'
go

sp_RENAME 'DATA_BB_20200731_EDM_FINAL_NEWKRITERIA.newFRELDATE' , 'newFRELDATE_NEWKRITERIA201708', 'COLUMN'
go

sp_RENAME 'DATA_BB_20200731_EDM_FINAL_NEWKRITERIA.freldt_Final' , 'freldt_Final_NEWKRITERIA201708', 'COLUMN'
go






drop table #PORTOMIX_NEW_KRITERIA
go

select
DATE_TIME,NO_CUS,ACCTNO as NO_REK,BUC_REPORTING,CBALRP_NET_PSAK,WILAYAH,WILAYAH_BARU,AREA,
KD_CAB,NM_CAB,NM_CUS,JNS_KRD,BIKOLE,ORGAMT,LIMIT,ACCSTS,STSDT,CLASCD,
MATDAT,FRELDT,
MTDPRNrp,MTDINTrp,MTDOTHrp,MTDLCrp,PRNDUE,INTDUE,DPD,REVOLV,
PURCOD,CURTYP,KURS,newORGAMT2016,ORGAMTbyNOcus2016,bucketLimitORGAMTbyNOcus201704,
bucketLimitORGAMTbyNOcus2M_2016,produk4,
CForgDate,NEW_MOBCIF,
dt,newORGAMT2016_LM,CBALRP_NET_PSAK_LM,
DeltaBade,
BISIFA,
BOOKING_NEWKRITERIA201708,
BOOKINGMIX_NEWKRITERIA201708 = 
case
when BOOKING_NEWKRITERIA201708 = 'DEBITUR BARU' then 'DEBITUR BARU'
when BOOKING_NEWKRITERIA201708 like '%TOP%UP%' then 'TOP UP'
when BOOKING_NEWKRITERIA201708 like '%PENARIKAN%' or BOOKING_NEWKRITERIA201708 like '%PENCAIRAN%' then 'PENARIKAN/PENCAIRAN'
else BOOKING_NEWKRITERIA201708
end,
NEW_PORTOMIX_NEWKRITERIA201708,
NEW_MIX_NEWKRITERIA201708 = 
case
when NEW_PORTOMIX_NEWKRITERIA201708 like '%PRIMA%' then '01. PRIMA'
when NEW_PORTOMIX_NEWKRITERIA201708 like '%VC%' then '02. VC'
when NEW_PORTOMIX_NEWKRITERIA201708 like '%koperasi%' or NEW_PORTOMIX_NEWKRITERIA201708 like '%kebun%plasma%' then '03. KOPERASI'
when NEW_PORTOMIX_NEWKRITERIA201708 like '%NTB%'then '04. Others - NTB'
when NEW_PORTOMIX_NEWKRITERIA201708 like '%ETB%'then '05. Others - ETB'
else NEW_PORTOMIX_NEWKRITERIA201708
end,
BADE_BOOKING_NEWKRITERIA201708,
newFRELDATE_NEWKRITERIA201708,
freldt_Final_NEWKRITERIA201708,
NEW_PRIMA_KRITERIA_201708,
NEW_VC_KRITERIA_201708,
NEW_KOPERASI_KRITERIA_201708,
MOB = datediff(month,FRELDT, date_time),
NEWMOB = datediff(month,freldt_Final_NEWKRITERIA201708, date_time),
bucket =
case
when isnull(dpd,0) = 0 then 'A. lancar'
when dpd between 1 and 30 then 'B. 1-30'
when dpd between 31 and 60 then 'C. 31-60'
when dpd between 61 and 90 then 'D. 61-90'
when dpd between 91 and 120 then 'E. 91-120'
when dpd between 121 and 150 then 'F. 121-150'
when dpd between 151 and 180 then 'G. 151-180'
when dpd between 181 and 210 then 'H. 181-210'
when dpd between 211 and 240 then 'I. 211-240'
when dpd between 241 and 270 then 'J. 241-270'
when dpd > 270 then 'K. > 270'
else null
end,
BADE_CLASD2B_5 = 
case
when CLASCD in ('2B','2C','03','04','05','3','4','5') then CBALRP_NET_PSAK
else null
end,

[BADE_DPD30+] = 
case
when isnull(dpd,0) > 30 then CBALRP_NET_PSAK
else null
end,

[BADE_NPL_BIKOLE] = 
case
when BIKOLE in ('03','04','05','3','4','5') then CBALRP_NET_PSAK
else null
end


--into #PORTOMIX_NEW_KRITERIA

FROM DATA_BB_20200731_EDM_FINAL_NEWKRITERIA
where year(date_time) ='2020' 
order by date_time,no_cus,acctno


