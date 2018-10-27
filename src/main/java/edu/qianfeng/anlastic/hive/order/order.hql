--总---订单数量：
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
and o.s_time >= unix_timestamp('2018-03-29','yyyy-MM-dd')*1000
and o.s_time <= unix_timestamp('2018-03-30','yyyy-MM-dd')*1000
group by o.pl,o.s_time,o.cut,o.pt
) as tmp
insert overwrite table ST_STATS_ORDER_TMP
select pl,dt,cut,pt,orders
insert overwrite table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert(pt),orders,dt
;

insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl,cut;
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert(cut),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,cut,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,cut;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt;

--sqoop statments
sqoop export --connect jdbc:mysql://hadoop01:3306/report \
--username root --password root --table stats_order \
--export-dir /hive/transformer.db/st_stats_order_tmp2/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created'
;


--成功支付---订单数量：
from(
select
o.pl as pl,
from_unixtime(cast(o.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
count(o.oid) as orders,
o1.cut as cut,
o1.pt as pt
from DW_ORDER o
join DW_ORDER o1
on o.oid = o1.oid
where o.pl is not null
and o.en='e_cs'
and o.s_time >= unix_timestamp('2018-03-29','yyyy-MM-dd')*1000
and o.s_time <= unix_timestamp('2018-03-30','yyyy-MM-dd')*1000
and o1.cut is not null and o1.pt is not null
group by o.pl,o.s_time,o1.cut,o1.pt
) as tmp
insert overwrite table ST_STATS_ORDER_TMP
select pl,dt,cut,pt,orders
insert overwrite table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert(pt),orders,dt
;

insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl,cut;
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert(cut),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,cut,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,cut;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt;

--sqoop statments
sqoop export --connect jdbc:mysql://hadoop01:3306/report \
--username root --password root --table stats_order \
--export-dir /hive/transformer.db/st_stats_order_tmp2/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created'
;

--成功退款---订单数量：
from(
select
o.pl as pl,
from_unixtime(cast(o.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
count(o.oid) as orders,
o1.cut as cut,
o1.pt as pt
from DW_ORDER o
join DW_ORDER o1
on o.oid = o1.oid
where o.pl is not null
and o.en='e_cr'
and o.s_time >= unix_timestamp('2018-03-29','yyyy-MM-dd')*1000
and o.s_time <= unix_timestamp('2018-03-30','yyyy-MM-dd')*1000
and o1.cut is not null and o1.pt is not null
group by o.pl,o.s_time,o1.cut,o1.pt
) as tmp
insert overwrite table ST_STATS_ORDER_TMP
select pl,dt,cut,pt,orders
insert overwrite table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert(pt),orders,dt
;

insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl,cut;
insert into table ST_STATS_ORDER_TMP2
select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pl;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert(cut),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,cut,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,pt;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt,cut;
insert into table ST_STATS_ORDER_TMP2
select platform_convert("all"),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(`value`),dt from ST_STATS_ORDER_TMP group by dt;

--sqoop statments
sqoop export --connect jdbc:mysql://hadoop01:3306/report \
 --username root --password root --table stats_order \
 --export-dir /hive/transformer.db/st_stats_order_tmp2/* \
 --input-fields-terminated-by "\\01" --update-mode allowinsert \
 --update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
 --columns 'platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created'
 ;


 --订单---总金额
 from(
 select
 e.pl as pl,
 from_unixtime(cast(e.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
 (case
 when currency_convert(e.cut) = 1 then cast(e.cua as bigint)
 when currency_convert(e.cut) = 3 then cast(cast(e.cua as bigint)*6.8 as bigint)
 end) as cua,
 e.cut as cut,
 e.pt as pt
 from dw_order e
 join dw_order o
 on e.oid = o.oid
 where e.pl is not null
 and e.en = 'e_crt'
 and e.s_time >= unix_timestamp("2018-03-29",'yyyy-MM-dd')*1000
 and e.s_time < unix_timestamp('2018-03-30','yyyy-MM-dd')*1000
 and o.cut is not null and o.pt is not null
 ) as tmp
 insert overwrite table st_stats_order_tmp
 select pl,dt,cut,pt,cua
 insert overwrite table st_stats_order_tmp2
 select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert(pt),sum(cua),dt group by pl,dt,cut,pt;
 insert into table st_stats_order_tmp2
 select platform_convert('all'),date_convert(dt),currency_convert(cut),payment_convert(pt),sum(`value`),dt from st_stats_order_tmp group by dt,cut,pt;
 insert into table st_stats_order_tmp2
 select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(`value`),dt from st_stats_order_tmp group by pl,dt,pt;
 insert into table st_stats_order_tmp2
 select platform_convert(pl),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(`value`),dt from st_stats_order_tmp group by pl,dt,cut;
 insert into table st_stats_order_tmp2
 select platform_convert('all'),date_convert(dt),currency_convert("all"),payment_convert(pt),sum(`value`),dt from st_stats_order_tmp  group by dt,pt;
 insert into table st_stats_order_tmp2
 select platform_convert('all'),date_convert(dt),currency_convert(cut),payment_convert("all"),sum(`value`),dt from st_stats_order_tmp group by dt,cut;
 insert into table st_stats_order_tmp2
 select platform_convert(pl),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(`value`),dt from st_stats_order_tmp  group by pl,dt;
 insert into table st_stats_order_tmp2
 select platform_convert('all'),date_convert(dt),currency_convert("all"),payment_convert("all"),sum(`value`),dt from st_stats_order_tmp group by dt;
 ;
 "



 --迄今为止的订单总金额
 from st_stats_order_tmp2
insert overwrite table st_stats_order_tmp3
select platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,`value`,created
;

from(
 select
 e.platform_dimension_id as platform_dimension_id,
 e.date_dimension_id as date_dimension_id,
 e.currency_type_dimension_id as currency_type_dimension_id,
 e.payment_type_dimension_id as payment_type_dimension_id,
 e.`value` as amount,
 e.created as dt
 from st_stats_order_tmp2 e
 ) as tmp
 insert overwrite table st_stats_order_tmp3
 select platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,amount+total_convert(cast(platform_dimension_id as int),cast(date_dimension_id as int),cast(currency_type_dimension_id as int),cast(payment_type_dimension_id as int)),dt
 ;






