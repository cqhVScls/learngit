#!/bin/bash

# ./en.sh -sd 2018-05-31 -ed 2018-06-01

startDate=
endate=

until [ $# -eq 0 ]
do
if [ $1'x' = '-sdx' ]
then
shift
startDate=$1
elif [ $1'x' = '-edx' ]
then
shift
endDate=$1
fi
shift
done

#判断startDate和endDate是否为空，为空则使用昨天为默认值
if [[ -n $startDate && -n  $endDate ]]
then
echo "startDate:${startDate};endDate:${endDate}"
else
echo "warning:it used default date."
startDate=`date -d  "1 days ago" "+%Y-%m-%d"`
endDate=$(date +%Y-%m-%d)
echo "startDate:${startDate};endDate:${endDate}"
fi

#开始运行hql
hive  -datebase gp1704 -e "
with tmp as(
select
e.pl as pl,
from_unixtime(cast(e.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
e.ca as ca,
e.ac as ac
from event_logs e
where e.en = 'e_e'
and e.pl is not null
and e.s_time >=  unix_timestamp("$startDate","yyyy-MM-dd")*1000
and e.s_time < unix_timestamp("$endDate","yyyy-MM-dd")*1000
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
";

#running sqoop statment
sqoop export --connect jdbc:mysql://hadoop01:3306/report \
--username root --password root --table stats_event \
--export-dir '/hive/gp1704.db/stats_event/*' \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id \
;

echo "event job completed."