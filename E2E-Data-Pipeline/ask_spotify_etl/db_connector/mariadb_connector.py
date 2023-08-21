import mariadb
from db_connector.db_connector import DatabaseConnector

class MariaDBConnector(DatabaseConnector):
    """blah
    """

    def __enter__(self):

        self.connector = mariadb.connect(host=self.database_credentials['host'],
                                        port=int(self.database_credentials['port']),
                                        user=self.database_credentials['user'],
                                        password=self.database_credentials['password'],
                                        database=self.database_credentials['database'])

        self.cursor = self.connector.cursor()


if __name__ == '__main__':

    connector = MariaDBConnector()
    with connector:
        pass