"""abstract parent class for DB connectors implemented as a ContextManager"""

class DatabaseConnector:
    """ 
    db connector parent class.
    implemented as a context manager.
    to me inherited by child classes with db specific connection in __enter__ method.
    """

    def __init__(self):
        self.connector = None
        self.cursor = None

    def __enter__(self):
        """
        method is executed when entering the WITH block.
        responsible for acquiring the necessary resources and setting up the context: DB Connection.
        To be overridden for specific db connector."""
        return self

    def execute(self, queries: list):
        """
        SQL queries execution logic.
        queries: list with sql query(s) is expected [sq(,...)]
        in case SELECT statement is executed, the values are returned.
        """

        records = list()

        if queries is None:
            return

        for query in queries:
            query = query.strip()

            if query.upper().startswith('SELECT') or query.upper().startswith('WITH'):
                self.cursor.execute(query)
                records.append(self.cursor.fetchall())
            else:
                self.cursor.execute(query)

        return records

    def __exit__(self, type, value, traceback):
        """
        This method is executed when exiting the with block.
        If traceback is not passed, the SQL query was executed successfully. The transaction must be committed.
        otherwise, the transaction must be rolled back.
        Close the connection.
        """
        if traceback is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.connector.close()
