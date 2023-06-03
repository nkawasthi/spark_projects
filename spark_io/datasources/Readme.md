# Data sources supported by Spark

Spark provides us the capability to read data from a variety of datasources which can be broadly categorized as RDBMS, NoSQL, Files, Streaming data etc
Looking more closely at the data sources, here as few examples for each category

1. RDBMS
   1. Postgres
   2. SQLServer
   3. MySQL
   4. AWS Redshift
2. NoSQL
   1. Hbase
   2. Cassendra
   3. MongoDB
3. Files
   1. S3 files/HDFS files
   2. CSV
   3. Parquet
   4. ORC
   5. Json
   6. XML
4. Streaming
   1. Kafka
   2. Flume
   3. Amazon Kinesis
5. Data Wareshouse 
   1. Hive
   2. Impala

The datasource module provides features to read,write and update data from the variety of sources supported by spark
The base class datasource.py is an abstract class which provides basic methods required for each datasource's I/O operations.
Each individual datasource class can then extend this abstract class to provide their own implementation.
The rationale behind creating an abstract class is to create a skeleton for all the implementing datasource classes to have a standard interface to read and write data.
