CREATE EXTERNAL TABLE IF NOT EXISTS posts_sample_external(
id INT,
post_type_id INT,
creation_date STRING
)
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
"input.regex" = '.*?(?=.*\\bId=\"(\\d+)\")(?=.*\\bPostTypeId=\"(\\d+)\")(?=.*\\bCreationDate=\"(\\S+)\").*'
)
LOCATION '/data/stackexchange1000/posts';



CREATE TABLE IF NOT EXISTS posts_sample(
id INT,
post_type_id INT,
creation_date STRING,
owner_user_id INT,
score INT,
tag_val ARRAY<STRING>
)
PARTITIONED BY (year INT, month INT);


INSERT OVERWRITE TABLE posts_sample
PARTITION (year, month)
SELECT id,post_type_id,creation_date,owner_user_id,score,split(regexp_replace(regexp_replace(tags, '&gt\;&lt\;', ','), '(&gt|&lt)\;', ''), ','),year(pse.creation_date) year, month(pse.creation_date) month
from posts_sample_external pse;


SELECT year, concat(year,'-',month), count(*)
from posts_sample
where id is not NULL
group by year, concat(year,'-',month);


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
LOCATION '/home/cloudera/hive_test';

DROP TABLE IF EXISTS posts_sample_external


select regexp_extract(tag_val,'(\&lt\;([^&gt\;]*)\&gt\;)+') from posts_sample_external_1;

select regexp_extract(tag_val,'(\&lt\;((?:(?!&gt\;).)*)\&gt\;)+') from posts_sample_external_1;


select regexp_extract(regexp_replace(regexp_replace(tag_val,'&lt\;','\<'),'&gt\;','\>'),'\<([\\S+]*)\>.*') from posts_sample_external_1;

select regexp_extract(regexp_replace(regexp_replace(tag_val,'&lt\;','\<'),'&gt\;','\>'),'\<([^\>]*)\>+') from posts_sample_external_1;


select id, split(regexp_replace(regexp_replace(tags, '&gt\;&lt\;', ','), '(&gt|&lt)\;', ''), ',') from posts_sample_external




