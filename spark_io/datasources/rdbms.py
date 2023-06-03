from datasources.datasource import DataSource
from pyspark.sql import DataFrame, SparkSession


class RDBMS(DataSource):

    def __init__(self):
        self.CONNECTION_PROVIDER = "connectionProvider"
        self.DRIVER_TYPE = "jdbc"
        self.URL = "url"
        self.HOST = "host"
        self.DRIVER = "driver"
        self.USER = "user"
        self.PASSWORD = "password"
        self.PORT = "port"
        self.DATABASE = "databaseName"
        self.PARTITION_COLUMN = "partitionColumn"
        self.LOWERBOUND = "lowerBound"
        self.UPPERBOUND = "upperBound"
        self.NUM_PARITIONS = "numPartitions"
        self.FETCH_SIZE = "fetchSize"
        self.DB_TABLE = "dbtable"
        self.TABLE = "table"
        self.SCHEMA = "schema"
        self.QUERY = "query"
        self.BATCH_SIZE = "batchSize"
        self.SAVE_MODE = "saveMode"
        self.SSL = "ssl"
        self.SSL_MODE = "sslmode"
        self.SSL_ROOT_CERT = "sslrootcert"
        self.REPARTITION = "repartition"

    def get_type(self, datasource_type: str) -> bool:
        if datasource_type.upper() == "RDBMS":
            return True
        else:
            return False

    def read_data(self, spark: SparkSession, config: dict) -> DataFrame:

        options = self.__create_read_options(table_config=config)
        df = spark.read\
            .format(self.DRIVER_TYPE)\
            .options(**options).load()
        return df

    def write_data(self, df: DataFrame, config: dict) -> None:
        """ Writes data to target """

        options = self.__create_write_options(table_config=config)
        save_mode = config.get(self.SAVE_MODE, "error")
        if self.REPARTITION in config:
            repartitions = int(config[self.REPARTITION])
            df.repartition(repartitions)\
                .write\
                .format(self.DRIVER_TYPE)\
                .mode(save_mode)\
                .options(**options)\
                .save()
        else:
            df.write \
                .format(self.DRIVER_TYPE) \
                .mode(save_mode) \
                .options(**options) \
                .save()

    def update_data(self, spark, config: dict) -> None:
        """ Updates Target Data """
        pass

    def _set_mandatory_jdbc_options(self, options: dict, table_config: dict) -> None:

        options[self.URL] = self.__make_jdbc_url(table_config)

        mandatory_config_list = [self.USER, self.PASSWORD, self.DB_TABLE, self.DRIVER]
        if set(mandatory_config_list) <= table_config.keys():
            # Check if given table configs dictionary contains all the mandatory configs required to read from JDBC
            # source
            try:
                for config_name in mandatory_config_list:
                    options[config_name] = table_config[config_name]
            except Exception as e:
                raise ValueError(f"One of the Mandatory config's value not found for "
                                 f"reading data from JDBC source.Mandatory read config are {mandatory_config_list}")

        else:
            raise ValueError(f"One of the Mandatory configs out of {mandatory_config_list} "
                             f"not found for spark JDBC read operation")

    def __make_jdbc_url(self, table_config: dict) -> str:
        mandatory_url_config_names = [self.CONNECTION_PROVIDER, self.HOST, self.PORT, self.DATABASE]
        # TODO implement better exception handling
        if set(mandatory_url_config_names) <= table_config.keys():
            # Check if given table configs dictionary contains all the mandatory configs required to create a JDBC URL
            try:
                url = f"{self.DRIVER_TYPE}:{table_config[self.CONNECTION_PROVIDER].lower()}://{table_config[self.HOST]}:{table_config[self.PORT]}/{table_config[self.DATABASE]}"
                return url
            except Exception as e:
                raise ValueError(f"One of the Mandatory config's value not found for "
                                 f"creating JDBC URL.Mandatory URLs config are {mandatory_url_config_names}")

        else:
            raise ValueError(f"One of the Mandatory configs out of {mandatory_url_config_names} "
                             f"not found for creating JDBC URL")

    def __create_read_options(self, table_config: dict) -> dict:
        options = {}

        table_config[self.DB_TABLE] = self.__get_dbtable_config(table_config, op="read")
        self._set_mandatory_jdbc_options(options, table_config)

        if self.PARTITION_COLUMN in table_config:
            options[self.PARTITION_COLUMN] = table_config[self.PARTITION_COLUMN]

        if self.LOWERBOUND in table_config:
            options[self.LOWERBOUND] = table_config[self.LOWERBOUND]

        if self.UPPERBOUND in table_config:
            options[self.UPPERBOUND] = table_config[self.UPPERBOUND]

        if self.NUM_PARITIONS in table_config:
            options[self.NUM_PARITIONS] = table_config[self.NUM_PARITIONS]

        if self.FETCH_SIZE in table_config:
            options[self.FETCH_SIZE] = table_config[self.FETCH_SIZE]

        if self.SSL in table_config:
            options[self.SSL] = table_config[self.SSL]

        if self.SSL_MODE in table_config:
            options[self.SSL_MODE] = table_config[self.SSL_MODE]

        if self.SSL_ROOT_CERT in table_config:
            options[self.SSL_ROOT_CERT] = table_config[self.SSL_ROOT_CERT]

        # TODO Add more options for different types of SSL connections
        # TODO Add support for predicates

        return options

    def __create_write_options(self, table_config: dict) -> dict:
        options = {}
        table_config[self.DB_TABLE] = self.__get_dbtable_config(table_config, op="write")
        self._set_mandatory_jdbc_options(options, table_config)

        if self.NUM_PARITIONS in table_config:
            options[self.NUM_PARITIONS] = table_config[self.NUM_PARITIONS]

        if self.BATCH_SIZE in table_config:
            options[self.BATCH_SIZE] = table_config[self.BATCH_SIZE]

        if self.SSL in table_config:
            options[self.SSL] = table_config[self.SSL]

        if self.SSL_MODE in table_config:
            options[self.SSL_MODE] = table_config[self.SSL_MODE]

        if self.SSL_ROOT_CERT in table_config:
            options[self.SSL_ROOT_CERT] = table_config[self.SSL_ROOT_CERT]

        # TODO Add more options for different types of SSL connections
        # TODO Add support for predicates

        return options

    def __get_dbtable_config(self, table_config: dict, op: str) -> str:
        if op == "write":
            dbtable = f"{table_config[self.SCHEMA]}.{table_config[self.TABLE]}"
            return dbtable
        elif op == "read":
            if self.QUERY in table_config:
                dbtable = table_config[self.QUERY]
                return dbtable
            else:
                table = '"{}"."{}"'.format(table_config[self.SCHEMA], table_config[self.TABLE])
                dbtable = "(select * from {} ) as table_query".format(table)
                return dbtable
