from ask_spotify_etl_config import Config

class DatabaseConnector:
    """ 
    blah
    """

    def __init__(self):
        self.database_credentials = Config().get("database.credentials")
        self.connector = None
        self.cursor = None

    def __enter__(self):
        """to be overridden for specific db connector"""
        return self

    def execute(self, queries: list, params=None):
        """
        """

        records = str()

        if queries is None:
            return

        for query in queries:
            query = query.strip()

            if query.upper().startswith('SELECT') or query.upper().startswith('WITH'):
                self.cursor.execute(query)
                records = self.cursor.fetchall()
            else:
                self.cursor.execute(query)

        return records

    def __exit__(self, exc_type, exc_val, exc_tb):
        """bar
        """
        if exc_tb is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()
