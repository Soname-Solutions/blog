import mariadb
from db_connector.db_connector import DatabaseConnector
from ask_spotify_etl_config import Config

class MariaDBConnector(DatabaseConnector):
    """
        MariaDB specific db connector.
        Inherited DB Connector as a context manager with SQL execute method.
    """
    def __init__(self, *args, **kwargs):
        super(MariaDBConnector, self).__init__(*args, **kwargs)
        self.database_credentials = Config().get("database.credentials")

    def __enter__(self):

        self.connector = mariadb.connect(host=self.database_credentials['host'],
                                        port=int(self.database_credentials['port']),
                                        user=self.database_credentials['user'],
                                        password=self.database_credentials['password'],
                                        database=self.database_credentials['database'])

        self.cursor = self.connector.cursor()
