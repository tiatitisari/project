
# PROBLEM B

# CHECK TABLE
SELECT * FROM `bigquery-public-data.new_york_citibike.citibike_stations` LIMIT 10;
SELECT * FROM `bigquery-public-data.new_york_citibike.citibike_trips` LIMIT 10
# Exclude trips with missing start_station_id from the trip table. 
# From the remaining trips, keep those with start_station_ids that were not present at the station table. 
# What percentage of these trips end up in end_station_ids which are also not present in the station table?
;
select (select count(end_station_id)
from `bigquery-public-data.new_york_citibike.citibike_trips` where start_station_id is not null and
end_station_id not in (select distinct station_id from `bigquery-public-data.new_york_citibike.citibike_stations`)
)/ --11032820
(select count(start_station_id)
from `bigquery-public-data.new_york_citibike.citibike_trips` where start_station_id is not null and
start_station_id not in (select distinct station_id from `bigquery-public-data.new_york_citibike.citibike_stations`)
); --11071784

select 11032820/11071784;--0.9964807839459295

# PROBLEM 2
# Filter the trip table to include only trips with starttime from 2018-01-01 onwards. 
--Combine usertype, birth_year, and gender into 1. Assume every unique combination represents 1 user. 
--Include users with missing usertype/birth_year/gender. 

# For every month, classify users into segments based on their trips data that month:
-- 0 distinct start_station_name = "inactive"
--1-10 distinct start_station_name = "casual”
-- > 10 distinct start_station_name = "power"
#Note that missing month data must be imputed with 0. For example,
-- if user A has info on months 1 and 3 but not 2, then you need to impute month 2 with 0 and therefore classify user A on month 2 as “inactive”.


#Questions:
--A. For each month in 2018, how many users belong to each segment?
--B. For each month in 2018, compute the movements of users between segments for the next month. 
--For example: from January 2018 to February 2018, how many casual users stayed as casual, became power, or became inactive? 
--Do the same for the other groups and the other months in 2018
WITH subQ1 as
(SELECT starttime, EXTRACT(MONTH FROM starttime) as month,
start_station_name,concat(cast(usertype as string),' ',cast(birth_year as string),' ',cast(gender as string)) as user FROM `bigquery-public-data.new_york_citibike.citibike_trips` where starttime>=    
'2018-01-01T00:00:00'),
    subQ2 as (select distinct a.month,b.user from subQ1 a cross join (select distinct user from subQ1)b),
    subQ3 as (select a.*, b.start_station_name from subQ2 a left join subQ1 b on a.month = b.month and a.user=b.user), 
    subQ4 as (select month, user, case when count(distinct start_station_name)=0 then 'inactive'
                                        when count(distinct start_station_name)<=10 then 'casual'
                                        when count(distinct start_station_name)>10 then 'power'
                                        else null end as segment from subQ3 group by month, user), 
    subQ5 as (select month,segment, count(distinct user) from subQ4 group by month, segment order by month), -- QuestionA,
    subQ6 as (select month,user,segment, case when segment = 'inactive' then 1
                                              when segment = 'casual' then 2
                                              when segment = 'power' then 4 else 0 end as segment2 from subQ4),
    subQ7 as(select a.month as initial_month,b.month as next_month,a.user,a.segment as segment_before,b.segment as segment_after, 
                            case when b.segment2-a.segment2 =0 then 'stay casual'
                                 when b.segment2 -a.segment2 =1 then 'change into casual'
                                 when b.segment2 -a.segment2 >=2 then 'change into power'
                                 when b.segment2 -a.segment2 >=-2 then 'change into casual'
                                 when b.segment2 - a.segment2 =-1 then 'change into inactive'
                                 else null end as total from subQ6 a inner join (select * from subQ6 where month>1) b on a.month = b.month-1 and a.user=b.user)
    select initial_month, next_month, total, count(distinct user) from subQ7 where segment_before = 'casual' group by initial_month, next_month, total;-- PROBLEM B
