Question 1
Problem Scenario 52 : You have been given below code snippet.
val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
Operation_xyz
Write a correct code snippet for Operation_xyz which will produce below output.
scalaxollection.Map[lnt,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> S, 2 -> 3, 4 -> 2, 7 ->
1)

Ans:

val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
val out = b.countByValue()



Question 2
Problem Scenario 81 : You have been given MySQL DB with following details. You have
been given following product.csv file
product.csv
productID,productCode,name,quantity,price
1001,PEN,Pen Red,5000,1.23
1002,PEN,Pen Blue,8000,1.25
1003,PEN,Pen Black,2000,1.25
1004,PEC,Pencil 2B,10000,0.48
1005,PEC,Pencil 2H,8000,0.49
1006,PEC,Pencil HB,0,9999.99
Now accomplish following activities.
1. Create a Hive ORC table using SparkSql
2. Load this data in Hive table.
3. Create a Hive parquet table using SparkSQL and load data in it.


Ans:

product = open("/home/cloudera/CDH_CCA/product_data/product.csv").read().splitlines()
productRdd = sc.parallelize(product)
productDF = productRdd.map(lambda x: (int(x.split(",")[0]), x.split(",")[1], x.split(",")[2], int(x.split(",")[3]), float(x.split(",")[4]))). \
            toDF(schema = ["productID", "productCode", "name","quantity","price"])

productDF.write.mode('overwrite').saveAsTable("product_orc_table", format = 'orc')

Option -1
---------
sqlContext.sql(' \
create external table product_tbl_ext ( \
product_id INT, \
product_code STRING, \
product_name STRING, \
product_quantity INT, \
product_price FLOAT \
) \
STORED AS ORC \
LOCATION "/user/hive/warehouse/cdh_cca.db/product_tbl_orc" ')

Option - 2
----------

sqlContext.sql(' \
create table product_tbl ( \
product_id INT, \
product_code STRING, \
product_name STRING, \
product_quantity INT, \
product_price FLOAT \
) \
STORED AS ORC')


sqlContext.sql('LOAD DATA INPATH "/user/hive/warehouse/cdh_cca.db/product_tbl_orc" INTO TABLE product_tbl')


sqlContext.sql(' \
create table product_tbl_parquet ( \
product_id INT, \
product_code STRING, \
product_name STRING, \
product_quantity INT, \
product_price FLOAT \
) \
STORED AS PARQUET')

sqlContext.sql('INSERT INTO product_tbl_parquet SELECT * from product_tbl')


Question 3
Problem Scenario 19 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Now accomplish following activities.
1. Import departments table from mysql to hdfs as textfile in departments_text directory.
2. Import departments table from mysql to hdfs as sequncefile in departments_sequence
directory.
3. Import departments table from mysql to hdfs as avro file in departments avro directory.
4. Import departments table from mysql to hdfs as parquet file in departments_parquet
directory.


Ans:

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--target-dir /user/cloudera/CDH_CCA/departments_text


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--as-sequencefile \
--target-dir /user/cloudera/CDH_CCA/departments_sequence


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--as-avrodatafile \
--target-dir /user/cloudera/CDH_CCA/departments_avro


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--as-parquetfile \
--target-dir /user/cloudera/CDH_CCA/departments_parquet




Question 4
Problem Scenario 58 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2) val b =
a.keyBy(_.length)
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, Seq[String])] = Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)),
(3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle)))

Ans:

b.groupByKey.collect()


Question 5
Problem Scenario 53 : You have been given below code snippet.
val a = sc.parallelize(1 to 10, 3)
operation1
b.collect
Output 1
Array[lnt] = Array(2, 4, 6, 8,10)
operation2
Output 2
Array[lnt] = Array(1,2, 3)
Write a correct code snippet for operation1 and operation2 which will produce desired
output, shown above.


Ans:

Operation1: val b = a.filter(x => x%2 == 0)

Operation2: a.filter(x => x < 4).collect()



Question 6
Problem Scenario 65 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
val b = sc.parallelize(1 to a.count.tolnt, 2)
val c = a.zip(b)
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))


Ans:


c.sortByKey(false).collect



Question 7
Problem Scenario 1:
You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following activities.
1. Connect MySQL DB and check the content of the tables.
2. Copy "retaildb.categories" table to hdfs, without specifying directory name.
3. Copy "retaildb.categories" table to hdfs, in a directory name "categories_target".
4. Copy "retaildb.categories" table to hdfs, in a warehouse directory name
"categories_warehouse".


Ans:

sqoop eval \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--query "select * from categories limit 10"



sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table categories \
--warehouse-dir /user/cloudera/CDH_CCA



sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table categories \
--target-dir /user/cloudera/CDH_CCA/categories_target


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table categories \
--warehouse-dir /user/cloudera/CDH_CCA/categories_warehouse



Question 8
Problem Scenario 63 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x))
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))


Ans:

b.reduceByKey((x,y) => x+y).collect()

Question 9
Problem Scenario 89 : You have been given below patient data in csv format,
patientID,name,dateOfBirth,lastVisitDate
1001,Ah Teck,1991-12-31,2012-01-20
1002,Kumar,2011-10-29,2012-09-20
1003,Ali,2011-01-30,2012-10-21
Accomplish following activities.
1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
2. Find all the patients who born in 2011
3. Find all the patients age
4. List patients whose last visited more than 60 days ago
5. Select patients 18 years old or younger


Ans:

patientRdd = sc.textFile("/user/cloudera/CDH_CCA/patient_data"). \
map(lambda x: (int(x.split(",")[0]),x.split(",")[1],x.split(",")[2], x.split(",")[3])). \
toDF(schema = ["patientID","name","dateOfBirth","lastVisitDate"])
patientRdd.registerTempTable("Patient")
1. sqlContext.sql("select * from Patient where lastVisitDate >= '2012-09-15' and lastVisitDate <= CURRENT_DATE").show()
2. sqlContext.sql("select * from Patient where date_format(dateOfBirth,'YYYY') = '2011'").show()
3. sqlContext.sql("select name, cast((unix_timestamp(CURRENT_DATE) - unix_timestamp(to_date(dateOfBirth)))/(3600*24*365) as int) as Age from Patient").show()
4. sqlContext.sql("select * from Patient where cast((unix_timestamp(CURRENT_DATE) - unix_timestamp(to_date(lastVisitDate)))/(3600*24) as int) > 60").show()
5. sqlContext.sql("select * from Patient where cast((unix_timestamp(CURRENT_DATE) - unix_timestamp(to_date(dateOfBirth)))/(3600*24*365) as int) <= 18").show()


Question 10
Problem Scenario 48 : You have been given below Python code snippet, with intermediate
output.
We want to take a list of records about people and then we want to sum up their ages and
count them.
So for this example the type in the RDD will be a Dictionary in the format of {name: NAME,
age:AGE, gender:GENDER}.
The result type will be a tuple that looks like so (Sum of Ages, Count)
people = []
people.append({'name':'Amit', 'age':45,'gender':'M'})
people.append({'name':'Ganga', 'age':43,'gender':'F'})
people.append({'name':'John', 'age':28,'gender':'M'})
people.append({'name':'Lolita', 'age':33,'gender':'F'})
people.append({'name':'Dont Know', 'age':18,'gender':'T'})
peopleRdd=sc.parallelize(people) //Create an RDD
peopleRdd.aggregate((0,0), seqOp, combOp) //Output of above line : 167, 5)
Now define two operation seqOp and combOp , such that
seqOp : Sum the age of all people as well count them, in each partition. combOp :
Combine results from all partitions.


Ans:


peopleRdd.aggregate((0,0), (lambda x, y : (x[0]+y['age'],x[1]+1)), (lambda x,y : (x[0]+y[0], x[1]+y[1])))


Question 11
Problem Scenario 15 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following activities.
1. In mysql departments table please insert following record. Insert into departments
values(9999, '"Data Science"1);
2. Now there is a downstream system which will process dumps of this file. However,
system is designed the way that it can process only files if fields are enlcosed in(') single
quote and separate of the field should be (-} and line needs to be terminated by : (colon).
3. If data itself contains the (double quote) than it should be escaped by.
4. Please import the departments table in a directory called departments_enclosedby and
file should be able to process by downstream system.


Ans:




sqoop eval \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--query "Insert into departments values(9999, \"Data Science\")"

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--enclosed-by '\"' \
--fields-terminated-by '-' \
--lines-terminated-by ':' \
--delete-target-dir \
--target-dir /user/cloudera/CDH_CCA/departments \
--escaped-by \\ 



Question 12
Problem Scenario 12 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following.
1. Create a table in retailedb with following definition.
CREATE table departments_new (department_id int(11), department_name varchar(45),
created_date T1MESTAMP DEFAULT NOW());
2. Now isert records from departments table to departments_new
3. Now import data from departments_new table to hdfs.
4. Insert following 5 records in departmentsnew table. Insert into departments_new
values(110, "Civil" , null); Insert into departments_new values(111, "Mechanical" , null);
Insert into departments_new values(112, "Automobile" , null); Insert into departments_new
values(113, "Pharma" , null);
Insert into departments_new values(114, "Social Engineering" , null);
5. Now do the incremental import based on created_date column.


Ans:


sqoop eval \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--query "CREATE table departments_new (department_id int(11), department_name varchar(45), created_date TIMESTAMP DEFAULT NOW())"



sqoop eval \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--query "INSERT INTO departments_new (department_id, department_name) SELECT * from departments"


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments_new \
--delete-target-dir \
--target-dir /user/cloudera/CDH_CCA/departments_new \
--num-mappers 1 \
--fields-terminated-by \\0xa4


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments_new \
--target-dir /user/cloudera/CDH_CCA/departments_new \
--num-mappers 1 \
--fields-terminated-by \\0xa4 \
--check-column created_date \
--incremental append \
--last-value '2018-11-08 19:00:00'




Question 13
Problem Scenario 32 : You have given three files as below.
spark3/sparkdir1/file1.txt
spark3/sparkd ir2ffile2.txt
spark3/sparkd ir3Zfile3.txt
Each file contain some text.
spark3/sparkdir1/file1.txt
Apache Hadoop is an open-source software framework written in Java for distributed
storage and distributed processing of very large data sets on computer clusters built from
commodity hardware. All the modules in Hadoop are designed with a fundamental
assumption that hardware failures are common and should be automatically handled by the
framework
spark3/sparkdir2/file2.txt
The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File
System (HDFS) and a processing part called MapReduce. Hadoop splits files into large
blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers
packaged code for nodes to process in parallel based on the data that needs to be
processed.
spark3/sparkdir3/file3.txt
his approach takes advantage of data locality nodes manipulating the data they have
access to to allow the dataset to be processed faster and more efficiently than it would be
in a more conventional supercomputer architecture that relies on a parallel file system
where computation and data are distributed via high-speed networking
Now write a Spark code in scala which will load all these three files from hdfs and do the
word count by filtering following words. And result should be sorted by word count in
reverse order.
Filter words ("a","the","an", "as", "a","with","this","these","is","are","in", "for",
"to","and","The","of")
Also please make sure you load all three files as a Single RDD (All three files must be
loaded using single API call).
You have also been given following codec
import org.apache.hadoop.io.compress.GzipCodec
Please use above codec to compress file, while saving in hdfs.




Ans:

hadoop fs -cp user/cloudera/CDH_CCA/Sparkdir/Sparkdir1/file1.txt user/cloudera/CDH_CCA/Sparkdir/
hadoop fs -cp user/cloudera/CDH_CCA/Sparkdir/Sparkdir2/file2.txt user/cloudera/CDH_CCA/Sparkdir/
hadoop fs -cp user/cloudera/CDH_CCA/Sparkdir/Sparkdir3/file3.txt user/cloudera/CDH_CCA/Sparkdir/


val textfile = sc.textFile("user/cloudera/CDH_CCA/Sparkdir/")

val textFileRdd = textfile.flatMap(x => x.split(" ")).map(x => x.trim())

val wordList = sc.parallelize(List("a","the","an", "as", "a","with","this","these","is","are","in", "for","to","and","The","of"))

val textRddOut = textFileRdd.subtract(wordList)

val wordCnt = textRddOut.map(x => (x,1)).reduceByKey((x, y) => x+y).map (x => (x._2, x._1)).sortByKey(false).map (x => (x._2 + " " +x._1))

wordCnt.saveAsTextFile("user/cloudera/CDH_CCA/Sparkdir/word_count",classOf[org.apache.hadoop.io.compress.GzipCodec])


Question 14
Problem Scenario 79 : You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of products table : (product_id | product categoryid | product_name |
product_description | product_prtce | product_image )
Please accomplish following activities.
1. Copy "retaildb.products" table to hdfs in a directory p93_products
2. Filter out all the empty prices
3. Sort all the products based on price in both ascending as well as descending order.
4. Sort all the products based on price as well as product_id in descending order.
5. Use the below functions to do data ordering or ranking and fetch top 10 elements top()
takeOrdered() sortByKey()


Ans:


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table products \
--delete-target-dir \
--target-dir /user/cloudera/CDH_CCA/p93_products \
--fields-terminated-by ":" \
--null-non-string -1


product = sc.textFile("/user/cloudera/CDH_CCA/p93_products")

productFiltered = product.filter (lambda x: x.split(":")[4] != -1)

productSortedAsc = productFiltered. \
map(lambda x : (float(x.split(":")[4]), x)). \
sortByKey(). \
map(lambda x: x[1])

productSortedDesc = productFiltered. \
map(lambda x : (float(x.split(":")[4]), x)). \
sortByKey(False). \
map(lambda x: x[1])

productSortedComb = productFiltered. \
map(lambda x : ((float(x.split(":")[4]),int(x.split(":")[0])), x)). \
sortByKey(False). \
map(lambda x: x[1])


productSortedComb = productFiltered. \
map(lambda x : ((float(x.split(":")[4]),int(x.split(":")[0])), x)). \
takeOrdered(5, key = lambda x: (-x[0][0],-x[0][1]))

productSortedComb = productFiltered. \
map(lambda x : ((float(x.split(":")[4]),int(x.split(":")[0])), x)). \
takeOrdered(5, key = lambda x: (x[0]))


productFiltered. \
map(lambda x : (float(x.split(":")[4]), x)). \
sortByKey(). \
takeOrdered(5,key = lambda x: x[0])



Question 15
Problem Scenario 80 : You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.products
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of products table : (product_id | product_category_id | product_name |
product_description | product_price | product_image )
Please accomplish following activities.
1. Copy "retaildb.products" table to hdfs in a directory p93_products
2. Now sort the products data sorted by product price per category, use productcategoryid
colunm to group by category


Ans:

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table products \
--delete-target-dir \
--target-dir /user/cloudera/CDH_CCA/p93_products \
--null-non-string -1 \
--null-string "NULL"


product = sc.textFile("/user/cloudera/CDH_CCA/p93_products")
filteredProduct = product.filter(lambda x: x.split(",")[4] != -1). \
filter(lambda x: x.split(",")[1] != -1). \
filter(lambda x: x.split(",")[4] != "")


sortedProduct = filteredProduct.map(lambda x: ((int(x.split(",")[1]),float(x.split(",")[4])),x)).\
sortByKey(). \
map(lambda x: x[1])


sortedProduct = filteredProduct.map(lambda x: (int(x.split(",")[1]),(int(x.split(",")[0]),float(x.split(",")[4])))).\
groupByKey(). \
map(lambda x: (x[0],sorted(x[1], key = lambda x: x[1], reverse = True)))


Question 16
Problem Scenario 10 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following.
1. Create a database named hadoopexam and then create a table named departments in
it, with following fields. department_id int,
department_name string
e.g. location should be
hdfs://quickstart.cloudera:8020/user/hive/warehouse/hadoopexam.db/departments
2. Please import data in existing table created above from retaidb.departments into hive
table hadoopexam.departments.
3. Please import data in a non-existing table, means while importing create hive table
named hadoopexam.departments_new


Ans:


create database hadoopexam;

use hadoopexam;

create table departments 
(
department_id int,
department_name string
)


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--hive-import \
--hive-table hadoopexam.departments

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--hive-import \
--hive-table hadoopexam.departments_new \
--create-hive-table


Question 17
Problem Scenario 28 : You need to implement near real time solutions for collecting
information when submitted in file with below
Data
echo "IBM,100,20160104" >> /tmp/spooldir2/.bb.txt
echo "IBM,103,20160105" >> /tmp/spooldir2/.bb.txt
mv /tmp/spooldir2/.bb.txt /tmp/spooldir2/bb.txt
After few mins
echo "IBM,100.2,20160104" >> /tmp/spooldir2/.dr.txt
echo "IBM,103.1,20160105" >> /tmp/spooldir2/.dr.txt
mv /tmp/spooldir2/.dr.txt /tmp/spooldir2/dr.txt
You have been given below directory location (if not available than create it) /tmp/spooldir2
As soon as file committed in this directory that needs to be available in hdfs in
/tmp/flume/primary as well as /tmp/flume/secondary location.
However, note that/tmp/flume/secondary is optional, if transaction failed which writes in
this directory need not to be rollback.
Write a flume configuration file named flumeS.conf and use it to load data in hdfs with
following additional properties .
1. Spool /tmp/spooldir2 directory
2. File prefix in hdfs sholuld be events
3. File suffix should be .log
4. If file is not committed and in use than it should have _ as prefix.
5. Data should be written as text to hdfs



Ans:


# example.conf: A single-node Flume configuration

# Name the components on this agent sps
sps.sources = sp
sps.sinks = hd1, hd2
sps.channels = mem1, mem2

# Describe/configure the source
sps.sources.sp.type = spooldir
sps.sources.sp.spoolDir = /tmp/spooldir2
sps.sources.sp.selector.type = replicating


# Describe the sinks
sps.sinks.hd1.type = hdfs
sps.sinks.hd1.hdfs.path = /tmp/flume/primary
sps.sinks.hd1.hdfs.filePrefix = events
sps.sinks.hd1.hdfs.fileSuffix = .log
sps.sinks.hd1.hdfs.fileType = DataStream

sps.sinks.hd2.type = hdfs
sps.sinks.hd2.hdfs.path = /tmp/flume/secondary
sps.sinks.hd2.hdfs.filePrefix = events
sps.sinks.hd2.hdfs.fileSuffix = .log
sps.sinks.hd2.hdfs.fileType = DataStream


# Use a channel which buffers events in memory
sps.channels.mem1.type = file
# sps.channels.mem1.capacity = 1000
# sps.channels.mem1.transactionCapacity = 100


sps.channels.mem2.type = file
# sps.channels.mem2.capacity = 1000
# sps.channels.mem2.transactionCapacity = 100

# Bind the source and sink to the channel
sps.sources.sp.channels = mem1, mem2
sps.sinks.hd1.channel = mem1
sps.sinks.hd2.channel = mem2


Question 18
Problem Scenario 95 : You have to run your Spark application on yarn with each executor
Maximum heap size to be 512MB and Number of processor cores to allocate on each
executor will be 1 and Your main application required three values as input arguments V1
V2 V3.
Please replace XXX, YYY, ZZZ
./bin/spark-submit -class com.hadoopexam.MyTask --master yarn-cluster--num-executors 3
--driver-memory 512m XXX YYY lib/hadoopexam.jarZZZ



Question 19
Problem Scenario 50 : You have been given below code snippet (calculating an average
score}, with intermediate output.
type ScoreCollector = (Int, Double)
type PersonScores = (String, (Int, Double))
val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0),
("Wilma", 95.0), ("Wilma", 98.0))
val wilmaAndFredScores = sc.parallelize(initialScores).cache()
val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner,
scoreMerger)
val averagingFunction = (personScore: PersonScores) => { val (name, (numberScores,
totalScore)) = personScore (name, totalScore / numberScores)}
val averageScores = scores.collectAsMap(}.map(averagingFunction)
Expected output: averageScores: scala.collection.Map[String,Double] = Map(Fred ->
91.33333333333333, Wilma -> 95.33333333333333)
Define all three required function , which are input for combineByKey method, e.g.
(createScoreCombiner, scoreCombiner, scoreMerger). And help us producing required
results.


Ans:

val scores = wilmaAndFredScores.combineByKey((x) => (x,1), (x: (Double,Int),y) => (x._1 + y, x._2+1), (x: (Double,Int),y:(Double,Int)) => (x._1+y._1, x._2+y._2))
scoreMerger)



Question 20
Problem Scenario 83 : In Continuation of previous question, please accomplish following
activities.
1. Select all the records with quantity >= 5000 and name starts with 'Pen'
2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with
'Pen'
3. Select all the records witch does not have quantity >= 5000 and name does not starts
with 'Pen'
4. Select all the products which name is 'Pen Red', 'Pen Black'
5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity
BETWEEN 1000 AND 2000.



Question 21
Problem Scenario 46 : You have been given belwo list in scala (name,sex,cost) for each
work done.
List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",
2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
Now write a Spark program to load this list as an RDD and do the sum of cost for
combination of name and sex (as key)


Ans:

val Lst = List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",
     | 2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))

val LstRdd = sc.parallelize(Lst)

val KVRdd = LstRdd.map({case (name,sex,cost) => (name,sex) -> cost})
val sumRdd = KVRdd.reduceByKey((x,y) => x+y).map({case((name,sex),sum) => (name,sex,sum)})
sumRdd.saveAsTextFile("spark12/result.txt")



Question 22
Problem Scenario 41 : You have been given below code snippet.
val aul = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))
Apply the Spark method, which will generate below output.
Array[(String, Array[lnt])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a(Array(3)), (b,Array(2)))

Ans:

au1.union(au2).collect()


Question 23
Problem Scenario 60 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","woif","bear","bee"), 3)
val d = c.keyBy(_.length)
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, (String, String))] = Array((6,(salmon,salmon)), (6,(salmon,rabbit)),
(6,(salmon,turkey)), (6,(salmon,salmon)), (6,(salmon,rabbit)),
(6,(salmon,turkey)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)),
(3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))

Ans:

b.join(d)


Question 24
Problem Scenario 13 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following.
1. Create a table in retailedb with following definition.
CREATE table departments_export (department_id int(11), department_name varchar(45),
created_date T1MESTAMP DEFAULT NOWQ);
2. Now import the data from following directory into departments_export table,
/user/cloudera/departments new



Ans:

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--delete-target-dir \
--target-dir /user/cloudera/departments_new


sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments_export \
--export-dir /user/cloudera/departments_new \
--batch



Question 25
Problem Scenario 23 : You have been given log generating service as below.
Start_logs (It will generate continuous logs)
Tail_logs (You can check , what logs are being generated)
Stop_logs (It will stop the log service)
Path where logs are generated using above service : /opt/gen_logs/logs/access.log
Now write a flume configuration file named flume3.conf , using that configuration file dumps
logs in HDFS file system in a directory called flumeflume3/%Y/%m/%d/%H/%M
Means every minute new directory should be created). Please us the interceptors to
provide timestamp information, if message header does not have header info.
And also note that you have to preserve existing timestamp, if message contains it. Flume
channel should have following property as well. After every 100 message it should be
committed, use non-durable/faster channel and it should be able to hold maximum 1000
events.


Ans:

agent1.sources = source1
agent1.sinks = sink1
agent1.channels = channel1


agent1.sources.source1.type = exec
agent1.sources.source1.command = tail -F /opt/gen_logs/logs/access.log
agent1.sources.source1.interceptors = i1
agent1.sources.source1.interceptors.i1.type = timestamp
agent1.sources.source1.interceptors.i1.preserveExisting = true


agent1.sinks.sink1.type = hdfs
agent1.sinks.sink1.hdfs.path = hdfs://quickstart.cloudera:8020/tmp/flume3/%Y/%m/%d/%H/%M
agent1.sinks.sink1.hdfs.fileType = DataStream
agent1.sinks.k1.hdfs.round = true
agent1.sinks.k1.hdfs.roundValue = 1
agent1.sinks.k1.hdfs.roundUnit = minute


agent1.channels.channel1.type = memory
agent1.channels.channel1.capacity = 1000
agent1.channels.channel1.transactionCapacity = 100


agent1.sources.source1.channels = channel1
agent1.sinks.sink1.channel = channel1


Question 28
Problem Scenario 43 : You have been given following code snippet.
val grouped = sc.parallelize(Seq(((1,twoM), List((3,4), (5,6)))))
val flattened = grouped.flatMap {A =>
groupValues.map { value => B }
You need to generate following output.
Hence replace A and B
Array((1,two,3,4),(1,two,5,6))


val grouped = sc.parallelize(Seq(((1,"two"), List((3,4), (5,6)))))


val flattened = grouped.flatMap ({case (a, (c,d)) => ((a,c),(a,d))})
groupValues.map { value => B }




Question 29
Problem Scenario 61 : You have been given below code snippet.
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
val d = c.keyBy(_.length) operationl
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, (String, Option[String]}}] = Array((6,(salmon,Some(salmon))),
(6,(salmon,Some(rabbit))),
(6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),
(6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))),
(3,(dog,Some(dog))), (3,(dog,Some(bee))), (3,(rat,Some(dogg)), (3,(rat,Some(cat)j),
(3,(rat.Some(gnu))). (3,(rat,Some(bee))), (8,(elephant,None)))


Ans:

b.leftOuterJoin(d).collect()


Question 31
Problem Scenario 4: You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.categories
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following activities.
Import Single table categories (Subset data} to hive managed table , where category_id
between 1 and 22


Ans:


sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--hive-import \
--hive-table cdh_cca.categories \
--create-hive-table \
--table categories \
--where "category_id >= 1 and category_id <=22"


Question 32
Problem Scenario 39 : You have been given two files
spark16/file1.txt
1,9,5
2,7,4
3,8,3
spark16/file2.txt
1,g,h
2,i,j
3,k,l
Load these two tiles as Spark RDD and join them to produce the below results
(l,((9,5),(g,h)))
(2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
And write code snippet which will sum the second columns of above joined results (5+4+3).


Ans:

file1 = open('/home/cloudera/CDH_CCA/temp/file1.txt').read().splitlines()
file2 = open('/home/cloudera/CDH_CCA/temp/file2.txt').read().splitlines()

file1Rdd = sc.parallelize(file1)
file2Rdd = sc.parallelize(file2)

file1KVRdd = file1Rdd.map(lambda x: (x.split(",")[0],(x.split(",")[1],x.split(",")[2])))
file2KVRdd = file2Rdd.map(lambda x: (x.split(",")[0],(x.split(",")[1],x.split(",")[2])))


joinRdd = file1KVRdd.join(file2KVRdd).sortByKey()

sumRdd = joinRdd.map(lambda x: (1,int(x[1][0][1]))). \
reduceByKey(lambda x,y : x+y). \
map(lambda x: x[1])


Question 33
Problem Scenario 17 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish below assignment.
1. Create a table in hive as below, create table departments_hive01(department_id int,
department_name string, avg_salary int);
2. Create another table in mysql using below statement CREATE TABLE IF NOT EXISTS
departments_hive01(id int, department_name varchar(45), avg_salary int);
3. Copy all the data from departments table to departments_hive01 using insert into
departments_hive01 select a.*, null from departments a;
Also insert following records as below
insert into departments_hive01 values(777, "Not known",1000);
insert into departments_hive01 values(8888, null,1000);
insert into departments_hive01 values(666, null,1100);
4. Now import data from mysql table departments_hive01 to this hive table. Please make
sure that data should be visible using below hive command. Also, while importing if null
value found for department_name column replace it with "" (empty string) and for id column
with -999 select * from departments_hive;


Ans:

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments_hive01 \
--hive-import \
--hive-table cdh_cca.departments_hive01 \
--null-non-string -999 \
--null-string "" \
--num-mappers 1



Question 34
Problem Scenario 21 : You have been given log generating service as below.
startjogs (It will generate continuous logs)
tailjogs (You can check , what logs are being generated)
stopjogs (It will stop the log service)
Path where logs are generated using above service : /opt/gen_logs/logs/access.log
Now write a flume configuration file named flumel.conf , using that configuration file dumps
logs in HDFS file system in a directory called flumel. Flume channel should have following
property as well. After every 100 message it should be committed, use non-durable/faster
channel and it should be able to hold maximum 1000 events
Solution :
Step 1 : Create flume configuration file, with below configuration for source, sink and
channel.
#Define source , sink , channel and agent,
agent1 .sources = source1
agent1 .sinks = sink1
agent1.channels = channel1
# Describe/configure source1
agent1 .sources.source1.type = exec
agent1.sources.source1.command = tail -F /opt/gen logs/logs/access.log
# Describe sinkl
agentl .sinks.sinkl.channel = memory-channel
agentl .sinks.sinkl .type = hdfs
agentl .sinks.sink1.hdfs.path = flumel
agentl .sinks.sinkl.hdfs.fileType = Data Stream
# Now we need to define channell property.
agent1.channels.channel1.type = memory
agent1.channels.channell.capacity = 1000
agent1.channels.channell.transactionCapacity = 100
# Bind the source and sink to the channel
agent1.sources.source1.channels = channel1
agent1.sinks.sink1.channel = channel1
Step 2 : Run below command which will use this configuration file and append data in
hdfs.
Start log service using : startjogs
Start flume service:
flume-ng agent -conf /home/cloudera/flumeconf -conf-file
/home/cloudera/flumeconf/flumel.conf-Dflume.root.logger=DEBUG,INFO,console
Wait for few mins and than stop log service.
Stop_logs



Ans:



agent1.sources = source1
agent1.sinks = sink1
agent1.channels = channel1


agent1.sources.source1.type = exec
agent1.sources.source1.command = tail -F /opt/gen_logs/logs/access.log


agent1.sinks.sink1.type = hdfs
agent1.sinks.sink1.hdfs.path = hdfs://quickstart.cloudera:8020/tmp/flume1
agent1.sinks.sink1.hdfs.fileType = DataStream


agent1.channels.channel1.type = memory
agent1.channels.channel1.capacity = 1000
agent1.channels.channel1.transactionCapacity = 100


agent1.sources.source1.channels = channel1
agent1.sinks.sink1.channel = channel1


flume-ng agent  --conf-file /home/cloudera/CDH_CCA/flume_Conf/flume1.conf --Dflume.root.logger=DEBUG,INFO,console


Question 35
Problem Scenario 67 : You have been given below code snippet.
lines = sc.parallelize(['lts fun to have fun,','but you have to know how.'])
M = lines.map( lambda x: x.replace(',7 ').replace('.',' ').replaceC-V ').lower())
r2 = r1.flatMap(lambda x: x.split())
r3 = r2.map(lambda x: (x, 1))
operation1
r5 = r4.map(lambda x:(x[1],x[0]))
r6 = r5.sortByKey(ascending=False)
r6.take(20)
Write a correct code snippet for operationl which will produce desired output, shown below.
[(2, 'fun'), (2, 'to'), (2, 'have'), (1, its'), (1, 'know1), (1, 'how1), (1, 'you'), (1, 'but')]

Ans:

r4 = r3.reduceByKey(lambda x, y : x + y)



Question 36
Problem Scenario 25 : You have been given below comma separated employee
information. That needs to be added in /home/cloudera/flumetest/in.txt file (to do tail
source)
sex,name,city
1,alok,mumbai
1,jatin,chennai
1,yogesh,kolkata
2,ragini,delhi
2,jyotsana,pune
1,valmiki,banglore
Create a flume conf file using fastest non-durable channel, which write data in hive
warehouse directory, in two separate tables called flumemaleemployee1 and
flumefemaleemployee1
(Create hive table as well for given data}. Please use tail source with
/home/cloudera/flumetest/in.txt file.
Flumemaleemployee1 : will contain only male employees data flumefemaleemployee1 :
Will contain only woman employees data


Ans:

create table flumemployee1 (
sex int,
name string,
city string
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';


agent1.sources = source1
agent1.sinks = sink1 sink2
agent1.channels = channel1 channel2


agent1.sources.source1.type = exec
agent1.sources.source1.command = tail -F /home/cloudera/flumetest/in.txt
agent1.sources.source1.interceptors = i1
agent1.sources.source1.interceptors.i1.type = regex_extractor
agent1.sources.source1.interceptors.i1.regex = ^(\\d)
agent1.sources.source1.interceptors.i1.serializers = s1
agent1.sources.source1.interceptors.i1.serializers.s1.name = sex
agent1.sources.source1.selector.type = multiplexing
agent1.sources.source1.selector.header = sex
agent1.sources.source1.selector.mapping.1 = channel1
agent1.sources.source1.selector.mapping.2 = channel2


agent1.sinks.sink1.type = hdfs
agent1.sinks.sink1.hdfs.path = hdfs://quickstart.cloudera:8020/user/hive/warehouse/cdh_cca.db/flumemployee1
agent1.sinks.sink1.hdfs.fileType = DataStream

agent1.sinks.sink2.type = hdfs
agent1.sinks.sink2.hdfs.path = hdfs://quickstart.cloudera:8020/user/hive/warehouse/cdh_cca.db/flumemployee1_1
agent1.sinks.sink2.hdfs.fileType = DataStream


agent1.channels.channel1.type = memory
agent1.channels.channel1.capacity = 1000
agent1.channels.channel1.transactionCapacity = 100

agent1.channels.channel2.type = memory
agent1.channels.channel2.capacity = 1000
agent1.channels.channel2.transactionCapacity = 100


agent1.sources.source1.channels = channel1 channel2
agent1.sinks.sink1.channel = channel1
agent1.sinks.sink2.channel = channel2



Question 38
Problem Scenario GG : You have been given below code snippet.
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
val b = a.keyBy(_.length)
val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
val d = c.keyBy(.length)
operation 1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, String)] = Array((4,lion))

Ans:

b.filter(x => x._1 == 4).collect()

b.subtractByKey(d).collect


Question 39
Problem Scenario 22 : You have been given below comma separated employee
information.
name,salary,sex,age
alok,100000,male,29
jatin,105000,male,32
yogesh,134000,male,39
ragini,112000,female,35
jyotsana,129000,female,39
valmiki,123000,male,29
Use the netcat service on port 44444, and nc above data line by line. Please do the
following activities.
1. Create a flume conf file using fastest channel, which write data in hive warehouse
directory, in a table called flumeemployee (Create hive table as well tor given data).
2. Write a hive query to read average salary of all employees.


Ans:

create table flumemployee (
sex int,
name string,
city string
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';


agent1.sources = source1
agent1.sinks = sink1
agent1.channels = channel1

agent1.sources.source1.type = netcat
agent1.sources.source1.bind = localhost
agent1.sources.source1.port = 44444


agent1.sinks.sink1.type = hdfs
agent1.sinks.sink1.hdfs.path = hdfs://quickstart.cloudera:8020/user/hive/warehouse/cdh_cca.db/flumemployee
agent1.sinks.sink1.hdfs.fileType = DataStream


agent1.channels.channel1.type = memory
agent1.channels.channel1.capacity = 1000
agent1.channels.channel1.transactionCapacity = 100

agent1.sources.source1.channels = channel1
agent1.sinks.sink1.channel = channel1


Question 40
Problem Scenario 8 : You have been given following mysql database details as well as
other info.
Please accomplish following.
1. Import joined result of orders and order_items table join on orders.order_id =
order_items.order_item_order_id.
2. Also make sure each tables file is partitioned in 2 files e.g. part-00000, part-00002
3. Also make sure you use orderid columns for sqoop to use for boundary conditions.


Ans:

sqoop import \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--query 'select a.*, b.* from orders a JOIN order_items b on (a.order_id = b.order_item_order_id) where $CONDITIONS' \
--num-mappers 2 \
--target-dir /user/cloudera/CDH_CCA/orders_joined \
--split-by a.order_id


Question 41
Problem Scenario 70 : Write down a Spark Application using Python, In which it read a
file "Content.txt" (On hdfs) with following content. Do the word count and save the
results in a directory called "problem85" (On hdfs)
Content.txt
Hello this is ABCTECH.com
This is XYZTECH.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce


Ans:

lines = sc.textFile('/user/cloudera/CDH_CCA/content.txt')
wordRdd = lines.flatMap(lambda x: x.split(" ")). \
map(lambda x: (x,1))
wordCount = wordRdd.reduceByKey(lambda x,y : x+y)
wordCount.saveAsTextFile('/user/cloudera/CDH_CCA/problem85')


Question 42
Problem Scenario 85 : In Continuation of previous question, please accomplish following
activities.
1. Select all the columns from product table with output header as below. productID AS ID
code AS Code name AS Description price AS 'Unit Price'
2. Select code and name both separated by ' -' and header name should be Product
Description'.
3. Select all distinct prices.
4. Select distinct price and name combination.
5. Select all price data sorted by both code and productID combination.
6. count number of products.
7. Count number of products for each code.



Question 43
Problem Scenario 77 : You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of order table : (orderid , order_date , order_customer_id, order_status)
Columns of ordeMtems table : (order_item_id , order_item_order_ld ,
order_item_product_id, order_item_quantity,order_item_subtotal,order_
item_product_price)
Please accomplish following activities.
1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory
p92_orders and p92 order items .
2. Join these data using orderid in Spark and Python
3. Calculate total revenue perday and per order
4. Calculate total and average revenue for each date. - combineByKey
-aggregateByKey


orders = sc.textFile('/user/cloudera/retail_db/orders')
orderItems = sc.textFile('/user/cloudera/retail_db/order_items')

ordersRdd = orders.map(lambda x: (int(x.split(",")[0]),x.split(",")[1]))
orderItemsRdd = orderItems.map(lambda x: (int(x.split(",")[1]), float(x.split(",")[4])))

revenueRerdayPerorder = ordersRdd.join(orderItemsRdd). \
map(lambda x: ((x[0],x[1][0]),x[1][1])). \
reduceByKey(lambda x, y : x+y). \
sortByKey()


avgRevenue = ordersRdd.join(orderItemsRdd). \
map(lambda x: (x[1][0],x[1][1])). \
aggregateByKey((0,0),(lambda x,y: (x[0]+y, x[1]+1)), (lambda x,y : (x[0]+y[0], x[1]+y[1]))). \
map(lambda x: (x[0], x[1][0]/x[1][1]))


avgRevenueComb = ordersRdd.join(orderItemsRdd). \
map(lambda x: (x[1][0],x[1][1])). \
combineByKey((lambda x: (x, 1)),(lambda x,y: (x[0]+y, x[1]+1)), (lambda x,y : (x[0]+y[0], x[1]+y[1]))). \
map(lambda x: (x[0], x[1][0]/x[1][1]))


Question 44
Problem Scenario 49 : You have been given below code snippet (do a sum of values by
key}, with intermediate output.
val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C",
"bar=D", "bar=D")
val data = sc.parallelize(keysWithValuesl_ist}
//Create key value pairs
val kv = data.map(_.split("=")).map(v => (v(0), v(l))).cache()
val initialCount = 0;
val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
Now define two functions (addToCounts, sumPartitionCounts) such, which will
produce following results.
Output 1
countByKey.collect
res3: Array[(String, Int)] = Array((foo,5), (bar,3))
import scala.collection._
val initialSet = scala.collection.mutable.HashSet.empty[String]
val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
Now define two functions (addToSet, mergePartitionSets) such, which will produce
following results.
Output 2:
uniqueByKey.collect
res4: Array[(String, scala.collection.mutable.HashSet[String])] = Array((foo,Set(B, A}},
(bar,Set(C, D}}}

Ans:

val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C",
"bar=D", "bar=D")
val data = sc.parallelize(keysWithValuesList)

val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

initialCount = 0

val countByKey = kv.aggregateByKey(initialCount)((x,y) => x+1, (x,y) => x+y)

import scala.collection._
val initialSet = scala.collection.mutable.HashSet.empty[String]

val uniqueByKey = kv.aggregateByKey(initialSet)((x: mutable.HashSet[String],y: String) => {x+=y}, (x: mutable.HashSet[String],y: mutable.HashSet[String]) => {x++=y})



Question 46
Problem Scenario 74 : You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of order table : (orderjd , order_date , ordercustomerid, order status}
Columns of orderjtems table : (order_item_td , order_item_order_id ,
order_item_product_id,
order_item_quantity,order_item_subtotal,order_item_product_price)
Please accomplish following activities.
1. Copy "retaildb.orders" and "retaildb.orderjtems" table to hdfs in respective directory
p89_orders and p89_order_items .
2. Join these data using orderjd in Spark and Python
3. Now fetch selected columns from joined data Orderld, Order date and amount collected
on this order.
4. Calculate total order placed for each date, and produced the output sorted by date.


Ans:

orders = sc.textFile('/user/cloudera/retail_db/orders')
orderItems = sc.textFile('/user/cloudera/retail_db/order_items')

ordersRdd = orders.map(lambda x: (int(x.split(",")[0]),x.split(",")[1]))
orderItemsRdd = orderItems.map(lambda x: (int(x.split(",")[1]), float(x.split(",")[4])))

revenueRerdayPerorder = ordersRdd.join(orderItemsRdd). \
map(lambda x: ((x[0],x[1][0]),x[1][1])). \
reduceByKey(lambda x, y : x+y). \
sortByKey()


totalRevenue = ordersRdd.join(orderItemsRdd). \
map(lambda x: (x[1][0],x[1][1])). \
aggregateByKey((0),(lambda x,y: x+1), (lambda x,y : x+y )). \
sortByKey()


Question 47
Problem Scenario 42 : You have been given a file (sparklO/sales.txt), with the content as
given in below.
spark10/sales.txt
Department,Designation,costToCompany,State
Sales,Trainee,12000,UP
Sales,Lead,32000,AP
Sales,Lead,32000,LA
Sales,Lead,32000,TN
Sales,Lead,32000,AP
Sales,Lead,32000,TN
Sales,Lead,32000,LA
Sales,Lead,32000,LA
Marketing,Associate,18000,TN
Marketing,Associate,18000,TN
HR,Manager,58000,TN
And want to produce the output as a csv with group by Department,Designation,State
with additional columns with sum(costToCompany) and TotalEmployeeCountt
Should get result like
Dept,Desg,state,empCount,totalCost
Sales,Lead,AP,2,64000
Sales.Lead.LA.3.96000
Sales,Lead,TN,2,64000


Ans:

sales = sc.textFile('/user/cloudera/CDH_CCA/sales.txt')
salesRdd = sales. \
map(lambda x: ((x.split(",")[0],x.split(",")[1],x.split(",")[3]),float(x.split(",")[2])))

salesAggr = salesRdd. \
aggregateByKey((0,0),(lambda x,y : (x[0]+1, x[1] + y)),(lambda x,y: (x[0]+y[0], x[1]+y[1]))). \
map(lambda x: x[0][0]+","+x[0][1]+","+x[0][2]+","+str(x[1][0])+","+str(x[1][1])). \
coalesce(1). \
saveAsTextFile('/user/cloudera/CDH_CCA/salesOut')


Question 48
Problem Scenario 76 : You have been given MySQL DB with following details.
user=retail_dba
password=cloudera
database=retail_db
table=retail_db.orders
table=retail_db.order_items
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Columns of order table : (orderid , order_date , ordercustomerid, order_status}
.....
Please accomplish following activities.
1. Copy "retail_db.orders" table to hdfs in a directory p91_orders.
2. Once data is copied to hdfs, using pyspark calculate the number of order for each status.
3. Use all the following methods to calculate the number of order for each status. (You
need to know all these functions and its behavior for real exam)
- countByKey()
-groupByKey()
- reduceByKey()
-aggregateByKey()
- combineByKey()



Ans:

orders = sc.textFile('/user/cloudera/retail_db/orders')
ordersRdd = orders. \
map(lambda x: (x.split(",")[3],x.split(",")[0])). \
countByKey()


ordersRdd_gk = orders. \
map(lambda x: (x.split(",")[3],1)). \
groupByKey(). \
map(lambda x: (x[0], (sum(x[1]))))


ordersRdd_rk = orders. \
map(lambda x: (x.split(",")[3],1)). \
reduceByKey(lambda x,y : x+y)


ordersRdd_ak = orders. \
map(lambda x: (x.split(",")[3],1)). \
aggregateByKey((0),(lambda x, y: x + y), (lambda x,y : x + y))


ordersRdd_ak = orders. \
map(lambda x: (x.split(",")[3],1)). \
aggregateByKey((0),(lambda x, y: x + y), (lambda x,y : x + y))


ordersRdd_ck = orders. \
map(lambda x: (x.split(",")[3],1)). \
combineByKey((lambda x: x),(lambda x, y: x + y), (lambda x,y : x + y))


Question 49
Problem Scenario 88 : You have been given below three files
product.csv (Create this file in hdfs)
productID,productCode,name,quantity,price,supplierid
1001,PEN,Pen Red,5000,1.23,501
1002,PEN,Pen Blue,8000,1.25,501
1003,PEN,Pen Black,2000,1.25,501
1004,PEC,Pencil 2B,10000,0.48,502
1005,PEC,Pencil 2H,8000,0.49,502
1006,PEC,Pencil HB,0,9999.99,502
2001,PEC,Pencil 3B,500,0.52,501
2002,PEC,Pencil 4B,200,0.62,501
2003,PEC,Pencil 5B,100,0.73,501
2004,PEC,Pencil 6B,500,0.47,502
supplier.csv
supplierid,name,phone
501,ABC Traders,88881111
502,XYZ Company,88882222
503,QQ Corp,88883333
products_suppliers.csv
productID,supplierID
2001,501
2002,501
2003,501
2004,502
2001,503
Now accomplish all the queries given in solution.
1. It is possible that, same product can be supplied by multiple supplier. Now find each
product, its price according to each supplier.
2. Find all the supllier name, who are supplying 'Pencil 3B'
3. Find all the products , which are supplied by ABC Traders.
4. Select product, its price , its supplier name where product price is less than 0.6 using
SparkSQL

Ans:

product = sc.textFile('/user/cloudera/CDH_CCA/product.csv')
supplier = sc.textFile('/user/cloudera/CDH_CCA/supplier.csv')

productDF = product. \
map(lambda x: (int(x.split(",")[0]), x.split(",")[1], x.split(",")[2], x.split(",")[3], float(x.split(",")[4]), int(x.split(",")[5]))). \
toDF(schema = ['productID', 'productCode', 'name', 'quantity', 'price', 'supplierid'])


productDF.registerTempTable('Product')


supplierDF = supplier. \
map(lambda x: (int(x.split(",")[0]), x.split(",")[1], x.split(",")[2])). \
toDF(schema = ['supplierid', 'name', 'phone'])

supplierDF.registerTempTable('Supplier')

sqlContext.sql('select productID, supplierid, price \
from Product \
order by productID').show()

sqlContext.sql("select b.name AS Supplier_Name \
from Product a JOIN \
Supplier b \
on a.supplierid = b.supplierid \
and a.name = \'Pencil 3B\'").show()


sqlContext.sql("select a.name AS Product_Name \
from Product a JOIN \
Supplier b \
on a.supplierid = b.supplierid \
and b.name = \'ABC Traders\'").show()


sqlContext.sql("select a.name AS Product_Name, a.price, b.name AS Supplier_Name \
from Product a JOIN \
Supplier b \
on a.supplierid = b.supplierid \
and a.price < 0.6").show()


Question 50
Problem Scenario 55 : You have been given below code snippet.
val pairRDDI = sc.parallelize(List( ("cat",2), ("cat", 5), ("book", 4),("cat", 12))) 
val pairRDD2 = sc.parallelize(List( ("cat",2), ("cup", 5), ("mouse", 4),("cat", 12)))
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(String, (Option[lnt], Option[lnt]))] = Array((book,(Some(4},None)),
(mouse,(None,Some(4))), (cup,(None,Some(5))), (cat,(Some(2),Some(2)),
(cat,(Some(2),Some(12))), (cat,(Some(5),Some(2))), (cat,(Some(5),Some(12))),
(cat,(Some(12),Some(2))), (cat,(Some(12),Some(12)))


Ans:

pairRDD1.fullOuterJoin(pairRDD2).collect()

Question 53
Problem Scenario 69 : Write down a Spark Application using Python,
In which it read a file "Content.txt" (On hdfs) with following content.
And filter out the word which is less than 2 characters and ignore all empty lines.
Once doen store the filtered data in a directory called "problem84" (On hdfs)
Content.txt
Hello this is ABCTECH.com
This is ABYTECH.com
Apache Spark Training
This is Spark Learning Session
Spark is faster than MapReduce

lines = sc.textFile('/user/cloudera/CDH_CCA/content.txt')
wordRdd = lines.filter(lambda x: len(x) > 0). \
flatMap(lambda x: x.split(" ")). \
filter(lambda x: len(x) > 2)
wordRdd.saveAsTextFile('/user/cloudera/CDH_CCA/problem84')


Question 55
Problem Scenario 90 : You have been given below two files
course.txt
id,course
1,Hadoop
2,Spark
3,HBase
fee.txt
id,fee
2,3900
3,4200
4,2900
Accomplish the following activities.
1. Select all the courses and their fees , whether fee is listed or not.
2. Select all the available fees and respective course. If course does not exists still list the
fee
3. Select all the courses and their fees , whether fee is listed or not. However, ignore
records having fee as null.

course = sc.textFile('/user/cloudera/CDH_CCA/course.txt')
fee = sc.textFile('/user/cloudera/CDH_CCA/fee.txt')

courseRdd = course. \
map(lambda x: (int(x.split(",")[0]), x.split(",")[1]))


feeRdd = fee. \
map(lambda x: (int(x.split(",")[0]), x.split(",")[1]))


joinRdd_lt = courseRdd. \
leftOuterJoin(feeRdd). \
map(lambda x: x[1])


joinRdd_rt = courseRdd. \
rightOuterJoin(feeRdd). \
map(lambda x: x[1])



joinRdd_ft = courseRdd. \
leftOuterJoin(feeRdd.filter(lambda x: len(x[1]) > 0)). \
map(lambda x: x[1])


Question 56
Problem Scenario 47 : You have been given below code snippet, with intermediate output.
val z = sc.parallelize(List(1,2,3,4,5,6), 2)
// lets first print out the contents of the RDD with partition labels
def myfunc(index: Int, iter: lterator[(lnt)]): lterator[String] = {
iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator
//In each run , output could be different, while solving problem assume belowm output only.
z.mapPartitionsWithlndex(myfunc).collect
res28: Array[String] = Array([partlD:0, val: 1], [partlD:0, val: 2], [partlD:0, val: 3], [partlD:1,
val: 4], [partlD:1, val: S], [partlD:1, val: 6])
Now apply aggregate method on RDD z , with two reduce function , first will select
max value in each partition and second will add all the maximum values from all
partitions.
Initialize the aggregate with value 5. hence expected output will be 16.

Ans:

val z = sc.parallelize(List(1,2,3,4,5,6), 2)

def myfunc(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator
}

z.mapPartitionsWithIndex(myfunc).collect



def myfunc_new(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
iter.toList.map(x => "(" + index + "," + x + ")").iterator
}

val zRdd = z.mapPartitionsWithIndex(myfunc_new). \
map((x))

zRdd.aggregateByKey(5)((x,y) => x+y,(x,y) => x+y)


Question 57
Problem Scenario 73 : You have been given data in json format as below.
{"first_name":"Ankit", "last_name":"Jain"}
{"first_name":"Amir", "last_name":"Khan"}
{"first_name":"Rajesh", "last_name":"Khanna"}
{"first_name":"Priynka", "last_name":"Chopra"}
{"first_name":"Kareena", "last_name":"Kapoor"}
{"first_name":"Lokesh", "last_name":"Yadav"}
Do the following activity
1. create employee.json file locally.
2. Load this file on hdfs
3. Register this data as a temp table in Spark using Python.
4. Write select query and print this data.
5. Now save back this selected data in json format.



employeeDF = sqlContext.read.json('/user/cloudera/CDH_CCA/name.json')

employeeDF.registerTempTable("Employee")

sqlContext.sql('select * from Employee').show()


sqlContext.sql('select * from Employee').write.format("json").save('/user/cloudera/CDH_CCA/p57')


Question 58
Problem Scenario 5 : You have been given following mysql database details.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following activities.
1. List all the tables using sqoop command from retail_db
2. Write simple sqoop eval command to check whether you have permission to read
database tables or not.
3. Import all the tables as avro files in /user/hive/warehouse/retail cca174.db
4. Import departments table as a text file in /user/cloudera/departments.



Ans:

sqoop eval \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--query 'show tables'


sqoop import-all-tables \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--as-avrodatafile \
--num-mappers 1 \
--warehouse-dir /user/hive/warehouse/retail_cca174.db



Question 60
Problem Scenario 30 : You have been given three csv files in hdfs as below.
EmployeeName.csv with the field (id, name)
EmployeeManager.csv (id, manager Name)
EmployeeSalary.csv (id, Salary)
Using Spark and its API you have to generate a joined output as below and save as a text
tile (Separated by comma) for final distribution and output must be sorted by id.
ld,name,salary,managerName
EmployeeManager.csv
E01,Vishnu
E02,Satyam
E03,Shiv
E04,Sundar
E05,John
E06,Pallavi
E07,Tanvir
E08,Shekhar
E09,Vinod
E10,Jitendra
EmployeeName.csv
E01,Lokesh
E02,Bhupesh
E03,Amit
E04,Ratan
E05,Dinesh
E06,Pavan
E07,Tejas
E08,Sheela
E09,Kumar
E10,Venkat
EmployeeSalary.csv
E01,50000
E02,50000
E03,45000
E04,45000
E05,50000
E06,45000
E07,50000
E08,10000
E09,10000
E10,10000


Ans:

employeeManager = sc.textFile('/user/cloudera/CDH_CCA/EmployeeManager.csv')
employeeName = sc.textFile('/user/cloudera/CDH_CCA/EmployeeName.csv')
employeeSalary = sc.textFile('/user/cloudera/CDH_CCA/EmployeeSalary.csv')



employeeManagerDF = employeeManager. \
map(lambda x: (x.split(",")[0], x.split(",")[1])). \
toDF(schema = ['Id', 'ManagerName'])

employeeManagerDF.registerTempTable("Emp_Mngr")


employeeNameDF = employeeName. \
map(lambda x: (x.split(",")[0], x.split(",")[1])). \
toDF(schema = ['Id', 'EmployeeName'])

employeeNameDF.registerTempTable("Emp_Name")



employeeSalaryDF = employeeSalary. \
map(lambda x: (x.split(",")[0], x.split(",")[1])). \
toDF(schema = ['Id', 'EmployeeSalary'])

employeeSalaryDF.registerTempTable("Emp_Salary")



sqlContext.sql('select a.Id, a.EmployeeName, c.EmployeeSalary, b.ManagerName \
from Emp_Name a \
JOIN Emp_Mngr b \
ON a.Id = b.Id \
JOIN Emp_Salary c \
ON a.Id = c.Id').show()


Question 62
Problem Scenario 27 : You need to implement near real time solutions for collecting
information when submitted in file with below information.
Data
echo "IBM,100,20160104" >> /tmp/spooldir/bb/.bb.txt
echo "IBM,103,20160105" >> /tmp/spooldir/bb/.bb.txt
mv /tmp/spooldir/bb/.bb.txt /tmp/spooldir/bb/bb.txt
After few mins
echo "IBM,100.2,20160104" >> /tmp/spooldir/dr/.dr.txt
echo "IBM,103.1,20160105" >> /tmp/spooldir/dr/.dr.txt
mv /tmp/spooldir/dr/.dr.txt /tmp/spooldir/dr/dr.txt
Requirements:
You have been given below directory location (if not available than create it) /tmp/spooldir .
You have a finacial subscription for getting stock prices from BloomBerg as well as
Reuters and using ftp you download every hour new files from their respective ftp site in
directories /tmp/spooldir/bb and /tmp/spooldir/dr respectively.
As soon as file committed in this directory that needs to be available in hdfs in
/tmp/flume/finance location in a single directory.
Write a flume configuration file named flume7.conf and use it to load data in hdfs with
following additional properties .
1. Spool /tmp/spooldir/bb and /tmp/spooldir/dr
2. File prefix in hdfs sholuld be events
3. File suffix should be .log
4. If file is not commited and in use than it should have _ as prefix.
5. Data should be written as text to hdfs


Ans:

sps.sources = sp1 sp2
sps.sinks = hd1
sps.channels = mem1


sps.sources.sp1.type = spooldir
sps.sources.sp1.spoolDir = /tmp/spooldir2/bb

sps.sources.sp2.type = spooldir
sps.sources.sp2.spoolDir = /tmp/spooldir2/dr


sps.sinks.hd1.type = hdfs
sps.sinks.hd1.hdfs.path = /tmp/flume/finance
sps.sinks.hd1.hdfs.filePrefix = events
sps.sinks.hd1.hdfs.fileSuffix = .log
sps.sinks.hd1.hdfs.fileType = DataStream


sps.channels.mem1.type = file
sps.channels.mem1.capacity = 1000
sps.channels.mem1.transactionCapacity = 100


sps.sources.sp1.channels = mem1
sps.sources.sp2.channels = mem1
sps.sinks.hd1.channel = mem1



echo "IBM,100,20160104" >> /tmp/spooldir2/bb/.bb.txt
echo "IBM,103,20160105" >> /tmp/spooldir2/bb/.bb.txt
mv /tmp/spooldir2/bb/.bb.txt /tmp/spooldir2/bb/bb.txt
After few mins
echo "IBM,100.2,20160104" >> /tmp/spooldir2/dr/.dr.txt
echo "IBM,103.1,20160105" >> /tmp/spooldir2/dr/.dr.txt
mv /tmp/spooldir2/dr/.dr.txt /tmp/spooldir2/dr/dr.txt



Question 63
Problem Scenario 57 : You have been given below code snippet.
val a = sc.parallelize(1 to 9, 3) operationl
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(String, Seq[lnt])] = Array((even,ArrayBuffer(2, 4, G, 8)), (odd,ArrayBuffer(1, 3, 5, 7,
9)))

Ans:


def isEven(x: Int) : String = {
if (x%2 == 0){
return "even"
}else{
return "odd"
}
}


val a = sc.parallelize(1 to 9, 3) 
val b = a.map((x) => (isEven(x),x))
b.groupByKey().collect()



Question 64
Problem Scenario 34 : You have given a file named spark6/user.csv.
Data is given below:
user.csv
id,topic,hits
Rahul,scala,120
Nikita,spark,80
Mithun,spark,1
myself,cca175,180
Now write a Spark code in scala which will remove the header part and create RDD of
values as below, for all rows. And also if id is myself than filter out row.
Map(id -> om, topic -> scala, hits -> 120)


Ans:

val user = sc.textFile("/Users/Sandip/Downloads/user/user.csv")
val userRdd = user.filter ((x) => x.split(",")(0) != "id").filter((x) => x.split(",")(0) != "myself")
userRdd.map((x) => (Map("id" -> x.split(",")(0), "topic" -> x.split(",")(1), "hits" -> x.split(",")(2)))).collect()


Question 66
Problem Scenario 2 :
There is a parent organization called "ABC Group Inc", which has two child companies
named Tech Inc and MPTech.
Both companies employee information is given in two separate text file as below. Please do
the following activity for employee details.
Tech Inc.txt
1,Alok,Hyderabad
2,Krish,Hongkong
3,Jyoti,Mumbai
4,Atul,Banglore
5,Ishan,Gurgaon
MPTech.txt
6,John,Newyork
7,alp2004,California
8,tellme,Mumbai
9,Gagan21,Pune
10,Mukesh,Chennai
1. Which command will you use to check all the available command line options on HDFS
and How will you get the Help for individual command.
2. Create a new Empty Directory named Employee using Command line. And also create
an empty file named in it Techinc.txt
3. Load both companies Employee data in Employee directory (How to override existing file
in HDFS).
4. Merge both the Employees data in a Single tile called MergedEmployee.txt, merged tiles
should have new line character at the end of each file content.
5. Upload merged file on HDFS and change the file permission on HDFS merged file, so
that owner and group member can read and write, other user can read the file.
6. Write a command to export the individual file as well as entire directory from HDFS to
local file System.

Ans:


hadoop fs -getmerge -nl /user/cloudera/CDH_CCA/employee MergedEmployee.txt



Question 82
Problem Scenario 40 : You have been given sample data as below in a file called
spark15/file1.txt
3070811,1963,1096,,"US","CA",,1,
3022811,1963,1096,,"US","CA",,1,56
3033811,1963,1096,,"US","CA",,1,23
Below is the code snippet to process this tile.
val field= sc.textFile("spark15/filel.txt")
val mapper = field.map(x=> A)
mapper.map(x => x.map(x=> {B})).collect
Please fill in A and B so it can generate below final output
Array(Array(3070811,1963,1096, 0, "US", "CA", 0,1, 0)
,Array(3022811,1963,1096, 0, "US", "CA", 0,1, 56)
,Array(3033811,1963,1096, 0, "US", "CA", 0,1, 23)



Ans:

val field= sc.textFile("/Users/Sandip/Downloads/user/file1.txt")

val mapper = field.map(x => x.split(","))

mapper.map(x => x.map(x=> {if (x == "") 0 else x})).collect



Question 85
Problem Scenario 14 : You have been given following mysql database details as well as
other info.
user=retail_dba
password=cloudera
database=retail_db
jdbc URL = jdbc:mysql://quickstart:3306/retail_db
Please accomplish following activities.
1. Create a csv file named updated_departments.csv with the following contents in local file
system.
updated_departments.csv
2,fitness
3,footwear
12,fathematics
13,fcience
14,engineering
1000,management
2. Upload this csv file to hdfs filesystem,
3. Now export this data from hdfs to mysql retaildb.departments table. During upload make
sure existing department will just updated and new departments needs to be inserted.
4. Now update updated_departments.csv file with below content.
2,Fitness
3,Footwear
12,Fathematics
13,Science
14,Engineering
1000,Management
2000,Quality Check
5. Now upload this file to hdfs.
6. Now export this data from hdfs to mysql retail_db.departments table. During upload
make sure existing department will just updated and no new departments needs to be
inserted.


Ans:

sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--update-key department_id \
--update-mode allowinsert \
--export-dir /user/cloudera/CDH_CCA/updated_departments


sqoop export \
--connect jdbc:mysql://quickstart:3306/retail_db \
--username retail_dba \
--password cloudera \
--table departments \
--update-key department_id \
--update-mode updateonly \
--export-dir /user/cloudera/CDH_CCA/updated_departments


Question 86
Problem Scenario 44 : You have been given 4 files , with the content as given below:
spark11/file1.txt
Apache Hadoop is an open-source software framework written in Java for distributed
storage and distributed processing of very large data sets on computer clusters built from
commodity hardware. All the modules in Hadoop are designed with a fundamental
assumption that hardware failures are common and should be automatically handled by the
framework
spark11/file2.txt
The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File
System (HDFS) and a processing part called MapReduce. Hadoop splits files into large
blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers
packaged code for nodes to process in parallel based on the data that needs to be
processed.
spark11/file3.txt
his approach takes advantage of data locality nodes manipulating the data they have
access to to allow the dataset to be processed faster and more efficiently than it would be
in a more conventional supercomputer architecture that relies on a parallel file system
where computation and data are distributed via high-speed networking
spark11/file4.txt
Apache Storm is focused on stream processing or what some call complex event
processing. Storm implements a fault tolerant method for performing a computation or
pipelining multiple computations on an event as it flows into a system. One might use
Storm to transform unstructured data as it flows into a system into a desired format
(spark11Afile1.txt)
(spark11/file2.txt)
(spark11/file3.txt)
(sparkl 1/file4.txt)
Write a Spark program, which will give you the highest occurring words in each file. With
their file name and highest occurring words.

Ans:

file = sc.textFile("user/cloudera/CDH_CCA/Sparkdir/file1.txt")
wordCountRdd = file.flatMap(lambda x: x.split(" ")). \
map(lambda x: (x,1)). \
reduceByKey(lambda x,y: x + y). \
map(lambda x: (x[1], x[0])). \
sortByKey(False)

maxWord = wordCountRdd.top(1)

y = file.name()

content1 = sc.parallelize(maxWord)

content1.map(lambda x: y+","+x[1]+","+str(x[0])).collect()



Question 92
Problem Scenario 26 : You need to implement near real time solutions for collecting
information when submitted in file with below information. You have been given below
directory location (if not available than create it) /tmp/nrtcontent. Assume your departments
upstream service is continuously committing data in this directory as a new file (not stream
of data, because it is near real time solution). As soon as file committed in this directory
that needs to be available in hdfs in /tmp/flume location
Data
echo "I am preparing for CCA175 from ABCTECH.com" > /tmp/nrtcontent/.he1.txt
mv /tmp/nrtcontent/.he1.txt /tmp/nrtcontent/he1.txt
After few mins
echo "I am preparing for CCA175 from TopTech.com" > /tmp/nrtcontent/.qt1.txt
mv /tmp/nrtcontent/.qt1.txt /tmp/nrtcontent/qt1.txt
Write a flume configuration file named flumes.conf and use it to load data in hdfs with
following additional properties.
1. Spool /tmp/nrtcontent
2. File prefix in hdfs sholuld be events
3. File suffix should be Jog
4. If file is not commited and in use than it should have as prefix.
5. Data should be written as text to hdfs



Ans:


sps.sources = sp
sps.sinks = hd2
sps.channels = mem1

sps.sources.sp.type = spooldir
sps.sources.sp.spoolDir = /tmp/nrtcontent
sps.sources.sp.selector.type = replicating

sps.sinks.hd2.type = hdfs
sps.sinks.hd2.hdfs.path = /user/cloudera/CDH_CCA/flume/p92
sps.sinks.hd2.hdfs.filePrefix = events
sps.sinks.hd2.hdfs.fileSuffix = .log
sps.sinks.hd2.hdfs.inUsePrefix = .
sps.sinks.hd2.hdfs.fileType = DataStream


sps.channels.mem1.type = memory
sps.channels.mem1.capacity = 1000
sps.channels.mem1.transactionCapacity = 100


sps.sources.sp.channels = mem1
sps.sinks.hd2.channel = mem1


Question 93
Problem Scenario 36 : You have been given a file named spark8/data.csv (type,name).
data.csv
1,Lokesh
2,Bhupesh
2,Amit
2,Ratan
2,Dinesh
1,Pavan
1,Tejas
2,Sheela
1,Kumar
1,Venkat
1. Load this file from hdfs and save it back as (id, (all names of same type)) in results
directory. However, make sure while saving it should be


Ans:

dataRdd = sc.textFile("/user/cloudera/CDH_CCA/data.csv")
dataMapRdd = dataRdd.map(lambda x: (int(x.split(",")[0]), x.split(",")[1])). \
groupByKey(). \
map(lambda x: (x[0], list(x[1])))


dataMapRdd = dataRdd.map(lambda x: (int(x.split(",")[0]), x.split(",")[1])). \
combineByKey((lambda x: x), (lambda x,y: x + ',' + y), (lambda x,y: x + ',' + y))


Question 95
Problem Scenario 59 : You have been given below code snippet.
val x = sc.parallelize(1 to 20)
val y = sc.parallelize(10 to 30) operationl
z.collect
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[lnt] = Array(16,12, 20,13,17,14,18,10,19,15,11)


Ans:

x.intersection(y).collect()


Question 96
Problem Scenario 62 : You have been given below code snippet.
val a = sc.parallelize(List("dogM", "tiger", "lion", "cat", "panther", "eagle"), 2)
val b = a.map(x => (x.length, x))
operation1
Write a correct code snippet for operationl which will produce desired output, shown below.
Array[(lnt, String)] = Array((3,xdogx), (5,xtigerx), (4,xlionx), (3,xcatx), (7,xpantherx),
(5,xeaglex))


Ans:

b.map((x) => (x._1,'X'+x._2+'X')).collect()


