1、编写货币类型和支付方式维度类
2、编写获取货币类型和支付方式维度ID的udf
create function currency_convert as 'edu.qianfeng.transform.hive.CurrencyTypeDimensionUdf' using jar 'hdfs://hadoop01:9000/transform/hive/udflib/GP1703_Transform-0.1.jar';
create function payment_convert as 'edu.qianfeng.transform.hive.PaymentTypeDimensionUdf' using jar 'hdfs://hadoop01:9000/transform/hive/udflib/GP1703_Transform-0.1.jar';
create function total_convert as 'edu.qianfeng.transform.hive.TotalAmountUdf' using jar 'hdfs://hadoop01:9000/transform/hive/udflib/GP1703_Transform-0.1.jar';



4、hql编写
创建hive表，然后映射对应的数据
create external table if not exists transformer.DW_ORDER(
 key String,
 pl String,
 s_time String,
 en String,
 oid string,
 `on` string,
 cua string,
 cut string,
 pt string
 )
 row format serde 'org.apache.hadoop.hive.hbase.HBaseSerDe'
 stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
 with serdeproperties('hbase.columns.mapping'=':key,info:pl,info:s_time,info:en,info:oid,info:on,info:cua,info:cut,info:pt')
 tblproperties('hbase.table.name'='event_logs')
 ;


5、创建临时表：
CREATE TABLE `ST_STATS_ORDER_TMP` (
  `pl` string,
  `dt` string,
  `cut` string,
  `pt` string,
  `value` int
);

创建临时表：
CREATE TABLE `ST_STATS_ORDER_TMP2` (
  platform_dimension_id bigint,
  date_dimension_id bigint,
  currency_type_dimension_id bigint,
  payment_type_dimension_id bigint,
  `value` int,
  created string
);

创建临时表：
CREATE TABLE `ST_STATS_ORDER_TMP3` (
  platform_dimension_id bigint,
  date_dimension_id bigint,
  currency_type_dimension_id bigint,
  payment_type_dimension_id bigint,
  `value` int,
  created string
);





6、总---订单数量：
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
--username root --password root --table stats_view_depth \
--export-dir /hive/transformer.db/stats_view_depth/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,kpi_dimension_id \
;
