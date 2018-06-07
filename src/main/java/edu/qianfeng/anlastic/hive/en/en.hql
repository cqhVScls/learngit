1、编写event的维度相关类
2、编写eventDimensionId的udf函数
3、创建hive的udfs函数，并将jar上传到HDFS中的指定位置
create function event_convert as 'edu.qianfeng.anlastic.hive.EventDimensionUDF' using jar 'hdfs://hadoop01:9000/qianfeng/anlastic/GP1704_AnlaysticLog-0.1.jar';
create function date_convert as 'edu.qianfeng.anlastic.hive.DateDimensionUDF' using jar 'hdfs://hadoop01:9000/qianfeng/anlastic/GP1704_AnlaysticLog-0.1.jar';
create function platform_convert as 'edu.qianfeng.anlastic.hive.PlatformDimensionUDF' using jar 'hdfs://hadoop01:9000/qianfeng/anlastic/GP1704_AnlaysticLog-0.1.jar';


4、创建hive的表
create external table if not exists event_logs(
key String,
pl String,
en String,
s_time String,
ca String,
ac String
)
row format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe'
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties('hbase.columns.mapping'=':key,info:pl,info:en,info:s_time,info:ca,info:ac')
tblproperties('hbase.table.name'='event_logs')
;

5、创建和结果表一样的临时表：
CREATE TABLE IF NOT EXISTS stats_event (
  platform_dimension_id int,
  date_dimension_id int,
  event_dimension_id int,
  times int,
  created String
  );

6、语句
with tmp as(
select
e.pl as pl,
from_unixtime(cast(e.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
e.ca as ca,
e.ac as ac
from event_logs e
where e.en = 'e_e'
and e.pl is not null
and e.s_time >=  unix_timestamp("2018-05-31","yyyy-MM-dd")*1000
and e.s_time < unix_timestamp("2018-06-01","yyyy-MM-dd")*1000
)
from (
select pl as pl,dt as dt,ca as ca,ac as ac,count(1) as times from tmp group by pl,dt,ca,ac union all
select pl as pl,dt as dt,ca as ca,'all' as ac,count(1) as times from tmp group by pl,dt,ca union all
select pl as pl,dt as dt,'all' as ca,'all' as ac,count(1) as times from tmp group by pl,dt union all
select 'all' as pl,dt as dt,ca as ca,ac as ac,count(1) as times from tmp group by dt,ca,ac union all
select 'all' as pl,dt as dt,ca as ca,'all' as ac,count(1) as times from tmp group by dt,ca union all
select 'all' as pl,dt as dt,'all' as ca,'all' as ac,count(1) as times from tmp group by dt
) as  tmp2
insert overwrite table stats_event
select  date_convert(dt),platform_convert(pl),event_convert(ca,ac),sum(times),dt
group by pl,dt,ca,ac
;

7、编写sqoop语句
sqoop export --connect jdbc:mysql://hadoop01:3306/report \
--username root --password root --table stats_event \
--export-dir '/hive/gp1704.db/stats_event/*' \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id \
;









