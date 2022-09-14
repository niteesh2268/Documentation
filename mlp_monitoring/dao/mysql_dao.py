from typeguard import typechecked
from mlp_monitoring.utils.db_connection import Connection


@typechecked
class MySQLDao:
    def __init__(self, env: str):
        self.env = env

    def insert(self, query: str, data: tuple):
        """
        :param query: Insert query to execute in MySQL
        :param data: Data to insert in the query
        :return: Number of rows inserted
        """
        my_sql_connection = Connection(self.env)
        try:
            my_sql_connection.cursor.execute(query, data)
            my_sql_connection.commit_query()
            return my_sql_connection.cursor.rowcount
        except Exception as e:
            return e
        finally:
            my_sql_connection.close_cursor()
            my_sql_connection.close_connection()

    def retrieve(self, query: str):
        """
        :param query: Select query to execute
        :return: List of rows
        """
        my_sql_connection = Connection(self.env)
        try:
            my_sql_connection.cursor.execute(query)
            return my_sql_connection.cursor.fetchall()
        except Exception as e:
            return e
        finally:
            my_sql_connection.close_cursor()
            my_sql_connection.close_connection()

    def update(self, query: str):
        """
        :param query: Update query to execute
        :return: Number of rows updated
        """
        my_sql_connection = Connection(self.env)
        try:
            my_sql_connection.cursor.execute(query)
            my_sql_connection.commit_query()
            return my_sql_connection.cursor.rowcount
        except Exception as e:
            return e
        finally:
            my_sql_connection.close_cursor()
            my_sql_connection.close_connection()

    def delete(self, query: str):
        """
        :param query: Delete query to execute
        :return: List of rows
        """
        my_sql_connection = Connection(self.env)
        try:
            my_sql_connection.cursor.execute(query)
            my_sql_connection.commit_query()
            return my_sql_connection.cursor.rowcount
        except Exception as e:
            return e
        finally:
            my_sql_connection.close_cursor()
            my_sql_connection.close_connection()

