1、创建表
create external table if not exists event_logs_vd(
  key String,
  pl String,
  en String,
  s_time String,
  uuid String,
  usdi String,
  url String
  )
  row format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe'
  stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
   with serdeproperties('hbase.columns.mapping'=':key,info:pl,info:en,info:s_time,info:u_ud,info:u_sd,info:p_url')
   tblproperties('hbase.table.name'='event_logs')
   ;

   2、编写hql语句
   CREATE TABLE `stats_view_depth` (
     `platform_dimension_id` int,
     `data_dimension_id` int,
     `kpi_dimension_id` int,
     `pv1` int,
     `pv2` int,
     `pv3` int,
     `pv4` int,
     `pv5_10` int,
     `pv10_30` int,
     `pv30_60` int,
     `pv60plus` int,
     `created` string
   );

   创建临时表：
   CREATE TABLE `stats_view_depth_tmp` (
     `pl` string,
     `dt` string,
     `col` string,
     `ct` bigint
   );

--##############################################浏览深度的用户指标###################################
--查询结果到临时表中
from(
select
v.pl as pl,
from_unixtime(cast(v.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
v.uuid as ud,
(case
when count(v.url) = 1 then "pv1"
when count(v.url) = 2 then "pv2"
when count(v.url) = 3 then "pv3"
when count(v.url) = 4 then "pv4"
when count(v.url) >= 5 and count(v.url) < 10 then "pv5_10"
when count(v.url) >= 10 and count(v.url) < 30 then "pv10_30"
when count(v.url) >= 30 and count(v.url) < 60 then "pv30_60"
else
"pv60plus"
end) as pv
from event_logs_vd v
where v.pl is not null
and v.en='e_pv'
and v.s_time >= unix_timestamp('2018-05-31','yyyy-MM-dd')*1000
and v.s_time <= unix_timestamp('2018-06-01','yyyy-MM-dd')*1000
group by v.pl,from_unixtime(cast(v.s_time/1000 as bigint),'yyyy-MM-dd'),v.uuid
) as tmp
insert overwrite table stats_view_depth_tmp
select pl,dt,pv,count(ud) as ct
where ud is not null
group by pl,dt,pv
;

#execut final result
with tmp as(
select pl as pl,dt as dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv1' union all
select pl as pl,dt as dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv2' union all
select pl as pl,dt as dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv3' union all
select pl as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv4' union all
select pl as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv5_10' union all
select pl as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv10_30' union all
select pl as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv30_60' union all
select pl as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60plus  from stats_view_depth_tmp where col = 'pv60plus' union all
select 'all' as pl,dt as dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv1' union all
select 'all' as pl,dt as dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv2' union all
select 'all' as pl,dt as dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv3' union all
select 'all' as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv4' union all
select 'all' as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv5_10' union all
select 'all' as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv10_30' union all
select 'all' as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60plus  from stats_view_depth_tmp where col = 'pv30_60' union all
select 'all' as pl,dt as dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60plus  from stats_view_depth_tmp where col = 'pv60plus'
)
insert overwrite table stats_view_depth
select platform_convert(pl),date_convert(dt),2,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60plus),dt from tmp
group by pl,dt
;

--sqoop statment
sqoop export --connect jdbc:mysql://hadoop01:3306/report \
 --username root --password root --table stats_view_depth \
 --export-dir /hive/gp1704.db/stats_view_depth/* \
 --input-fields-terminated-by "\\01" --update-mode allowinsert \
 --update-key platform_dimension_id,date_dimension_id,kpi_dimension_id \
 ;





