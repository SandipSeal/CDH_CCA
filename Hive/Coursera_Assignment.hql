%%writefile exteral_table.hql

ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE stackoverflow_;

DROP TABLE posts_sample_external;

CREATE EXTERNAL TABLE IF NOT EXISTS posts_sample_external(
id INT,
post_type_id INT,
creation_date STRING,
owner_user_id INT,
score INT,
tag_val STRING
)
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
"input.regex" = '.*?(?=.*\\bId=\"(\\d+)\")(?=.*\\bPostTypeId=\"(\\d+)\")(?=.*\\bCreationDate=\"(\\S+)\")(?=.*\\bOwnerUserId=\"(\\d+)\")(?=.*\\bScore=\"(\\d+)\")?(?=.*\\bTags=\"(\\S+)\")?.*'
)
LOCATION '/data/stackexchange1000/posts';

%%writefile posts_table.hql

ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE stackoverflow_;

DROP TABLE posts_sample;

CREATE TABLE IF NOT EXISTS posts_sample(
id INT,
post_type_id INT,
creation_date STRING,
owner_user_id INT,
score INT,
tag_val ARRAY<STRING>
)
PARTITIONED BY (year INT, month INT);



%%writefile posts_table_insert.hql

ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE stackoverflow_;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000; 

INSERT OVERWRITE TABLE posts_sample
PARTITION (year, month)
SELECT id,post_type_id,creation_date,owner_user_id,score,split(regexp_replace(regexp_replace(tag_val, '&gt\;&lt\;', ','), '(&gt|&lt)\;', ''), ','),year(pse.creation_date) year, month(pse.creation_date) month
from posts_sample_external pse;


%%writefile posts_query.hql

ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
USE stackoverflow_;


SELECT a.tag, a.rank, b.rank
FROM
(SELECT '2016' AS year, tag, count, RANK() OVER (ORDER BY count DESC) AS rank
FROM
(SELECT tag, COUNT(1) AS count FROM(SELECT explode(tag_val) AS tag FROM posts_sample where year = 2016) t
GROUP BY tag)w)a
JOIN 
(SELECT '2009' AS year, tag, count, RANK() OVER (ORDER BY count DESC) AS rank
FROM
(SELECT tag, COUNT(1) AS count FROM(SELECT explode(tag_val) AS tag FROM posts_sample where year = 2009) t
GROUP BY tag)w) b
ON a.tag = b.tag
limit 10;





