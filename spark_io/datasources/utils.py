from datasources.rdbms import RDBMS


def get_datasource_obj(database_type):
    datasource_providers = [RDBMS]
    try:
        datasource_type = filter(
            lambda datasource_provider: datasource_provider().get_type(database_type), datasource_providers)

        datasource_type_list = list(datasource_type)
        if datasource_type_list:
            return datasource_type_list[0]()
        else:
            raise Exception(f"No Dialect defined to support given url dialect type {database_type}")
    except Exception as e:
        print(f"An Exception occured while getting Data Source Provider object: {e}")
