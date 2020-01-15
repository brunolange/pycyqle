import mysql.connector
import getpass


class MySQLConnector():
    def __init__(self, conn):
        self._conn = conn
        self._cursor = conn.cursor(dictionary=True)

    def execute(self, query, binds={}):
        return self._cursor.execute(query, binds)

    def data(self):
        return self._cursor.fetchall()

    def close(self):
        self._conn.close()

    @staticmethod
    def cli_build():
        config = {
            'user': 'root',
            'password': getpass.getpass(),
            'host': '127.0.0.1',
            'database': 'playground'
        }
        return MySQLConnector.build(config)

    @staticmethod
    def build(config):
        conn = mysql.connector.connect(**config)
        return MySQLConnector(conn)
