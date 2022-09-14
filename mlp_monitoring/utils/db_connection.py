from typeguard import typechecked
import mysql.connector
from mlp_monitoring.constant.config import Config


def _create_connection(env: str):
    """
    :param env: Env of db to connect
    :return: MySQL connector
    """
    return mysql.connector.connect(
        host=Config(env).db_master_url,
        database=Config(env).db_name,
        user=Config(env).db_user,
        password=Config(env).db_password,
        port='3306'
    )


@typechecked
class Connection:
    def __init__(self, env: str):
        self.connector = _create_connection(env)
        self.cursor = self.connector.cursor(dictionary=True)

    def commit_query(self):
        """
        Commits to MySQL DB
        """
        self.connector.commit()

    def close_cursor(self):
        """
        Closes MySQL cursor
        """
        self.cursor.close()

    def close_connection(self):
        """
        Closes MySQL connection
        """
        self.connector.close()
