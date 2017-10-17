from common.market_config import MarketConfig

class DataSourceFactory:

    @staticmethod
    def get_data_source():
        type = MarketConfig.get('DATA_SOURCE', 'TYPE')
        if (type == "arctic"):
            from common.arctic_data_source import ArcticDataSource
            return ArcticDataSource(mongo_connect = MarketConfig.get('DATA_SOURCE', 'ARCTIC_CONNECT'))
        else:
            raise NotImplementedError