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