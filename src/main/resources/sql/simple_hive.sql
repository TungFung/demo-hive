CREATE DATABASE IF NOT EXISTS twq;

use twq;

-根据我们在spark-rdd模块中定义的avro文件中的schema创建表，这个时候hive的metastore状态会变
-mysql中的hive数据库的表`TBLS`会新增一条记录
-且hive的数据warehouse中会新增一个文件目录：/user/hive/warehouse/twq.db/tracker_session
CREATE TABLE IF NOT EXISTS twq.tracker_session (
 session_id string,
 session_server_time string,
 cookie string,
 cookie_label string,
 ip string,
 landing_url string,
 pageview_count int,
 click_count int,
 domain string,
 domain_label string)
STORED AS PARQUET;

-将数据load到表tracker_session中
-/user/hadoop-twq/example/trackerSession下的数据会被move到/user/hive/warehouse/tracker_session中
LOAD DATA INPATH 'hdfs://master:9999/twq/trackerSession' OVERWRITE INTO TABLE twq.tracker_session;

-查询表tracker_session
-根据hive的配置，先去metastore中查询相关的表的元数据信息，然后再去warehouser下面查询相关的数据信息
select count(*) from twq.tracker_session;
select * from twq.tracker_session;
select cookie, count(*) from twq.tracker_session group by cookie;

drop table tracker_session;

drop database twq;

