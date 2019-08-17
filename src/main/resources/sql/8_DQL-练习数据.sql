-- 练习创建复杂结构的表
create table test1(
    id VARCHAR(32),
    type ARRAY<VARCHAR(10)>,
    tags MAP<VARCHAR(10),INT>,
    salesman STRUCT<id:VARCHAR(32), name:VARCHAR(32), dept_name:VARCHAR(32)>
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/hadoop-twq/myData/test1_data.txt' OVERWRITE INTO TABLE test1;
A001    中式,美式,英式    中式价格:100,美式价格:98,英式价格:102   S001,王五,销售一部
A002    中式,美式,英式    中式价格:100,美式价格:98,英式价格:102   S002,赵柳,销售一部

select salesman.id,tags['英式价格'],type[1] from test1;

-- 练习HQL
-- grouping__id
-- grouping sets with rollup
-- grouping sets with cube
-- case when 表内转换
-- first_value & last_value
-- lead & lag
-- row_number & rank & dense_rank & percent_rank
-- ntile
-- cume_dist

-- 统计一个月内连续三天登录的用户
create table tb_user(
    user_id string,
    login_date date
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

user1   2019-01-18
user1   2019-01-17
user1   2019-01-16
user1   2019-01-16
user1   2019-01-16
user1	2019-02-10
user2	2019-02-10
user1	2019-02-11
user1	2019-02-11
user1	2019-02-11
user1	2019-02-11
user1	2019-02-12
user3	2019-02-12
user2	2019-02-12
user2	2019-02-13
user3	2019-02-13