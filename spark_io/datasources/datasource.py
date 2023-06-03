from abc import ABC, abstractmethod


class DataSource(ABC):

    @abstractmethod
    def read_data(self, spark, config):
        """ Reads data from source """

    @abstractmethod
    def write_data(self, df, config):
        """ Writes data to target """

    @abstractmethod
    def update_data(self, spark, config):
        """ Updates Target Data """
