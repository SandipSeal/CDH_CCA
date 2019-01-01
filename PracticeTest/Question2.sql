create table product_tbl (
product_id INT,
product_code STRING,
product_name STRING,
product_quantity INT,
product_price FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE;


create table product_tbl_orc (
product_id INT,
product_code STRING,
product_name STRING,
product_quantity INT,
product_price FLOAT
)
STORED AS ORC;