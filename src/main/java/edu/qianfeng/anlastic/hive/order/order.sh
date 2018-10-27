#!/bin/bash

start_date=''
end_date=''
#parser arguments     ./en.sh -sd 2018-03-28 -ed 2018-03-29
until [ $# -eq 0 ]
do
    if [ $1'x' = '-sdx' ]
        then
        shift
        start_date=$1
    elif [ $1'x' = '-edx' ]
         then
         shift
         end_date=$1
    fi
    shift
done

#set default date
if [ -n "$start_date" ] && [ -n "$end_date" ]
    then
    echo "start_date and end_date is ok. start_date:${start_date}.end_date:${end_date}"
else
    start_date=`date -d "1 days ago" '+%Y-%m-%d'`
    end_date=`date -d "0 days ago" '+%Y-%m-%d'`
    echo "aaaaa"
fi

echo "before job running:"start_date:${start_date}.end_date:${end_date}
echo #####################################
echo #######
echo ####### start running order_hql and sqoop statsment
echo #######
echo ######################################

#execut order.hql
hive -database transformer -e "
from(
select
o.pl as pl,
from_unixtime(cast(o.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
count(o.oid) as orders,
o.cut as cut,
o.pt as pt
from DW_ORDER o
where o.pl is not null
and o.en='e_crt'
and o.s_time >= unix_timestamp('${start_date}','yyyy-MM-dd')*1000
and o.s_time <= unix_timestamp('${end_date}','yyyy-MM-dd')*1000
group by o.pl,o.s_time,o.cut,o.pt
) as tmp
insert overwrite table ST_STATS_ORDER_TMP
select pl,dt,cut,pt,orders
insert overwrite table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert(pt),orders,dt
;
"

hive -database transformer -e "
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(value),dt from ST_STATS_ORDER_TMP group by dt,pl,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(value),dt from ST_STATS_ORDER_TMP group by dt,pl,cut;
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(value),dt from ST_STATS_ORDER_TMP group by dt,pl;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert(cut),payment_convert(pt),sum(value),dt from ST_STATS_ORDER_TMP group by dt,cut,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(value),dt from ST_STATS_ORDER_TMP group by dt,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(value),dt from ST_STATS_ORDER_TMP group by dt,cut;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(value),dt from ST_STATS_ORDER_TMP group by dt;
"

#execute sqoop
sqoop export --connect jdbc:mysql://hadoop01:3306/report \
--username root --password root --table stats_order \
--export-dir /hive/transformer.db/st_stats_order_tmp2/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created'
;

echo "order_hql and sqoop statsment is ended"