from pyspark.sql import SparkSession, DataFrame
from datasources.utils import get_datasource_obj

if __name__ == "__main__":
    # Read Data from Postgres
    spark = SparkSession.builder.master("local[1]").appName("apps").config("spark.jars", "/Users/neha.awasthi/Downloads/postgresql-42.3.7.jar").getOrCreate()

    database_type = "rdbms"
    spark_io_obj = get_datasource_obj(database_type)

    read_table_config = {"connectionProvider": "postgresql",
                         "host": "localhost",
                         "driver": "org.postgresql.Driver",
                         "user": "postgres",
                         "password": "postgres",
                         "port": "5432",
                         "databaseName": "test",
                         "schema": "public",
                         "table": "test_table"}

    df: DataFrame = spark_io_obj.read_data(spark=spark, config=read_table_config)

    write_table_config = {"connectionProvider": "postgresql",
                         "host": "localhost",
                         "driver": "org.postgresql.Driver",
                         "user": "postgres",
                         "password": "postgres",
                         "port": "5432",
                         "databaseName": "test",
                         "schema": "public",
                         "table": "test_table_target"}

    df.show()

    spark_io_obj.write_data(df=df, config=write_table_config)
