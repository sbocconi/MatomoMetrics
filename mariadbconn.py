import mariadb
import sys
from os import system

class MariaDBConn:
    def __init__(self, user:str, database:str, host:str=None, socket:str=None, port:int=3306):
        self.host = host
        self.port = port
        self.socket = socket
        self.database = database
        self.user = user
        self.conn = None

    def load_data(self, filename):
        command = f'mysql -u {self.user} < {filename}'
        system(command)

    def connect(self, password):
        try:
            if self.host is not None:
                self.conn = mariadb.connect(
                    user=self.user,
                    password=password,
                    host=self.host,
                    port=self.port,
                    database=self.database

                )
            else:
                self.conn = mariadb.connect(
                    user=self.user,
                    password=password,
                    unix_socket=self.socket,
                    database=self.database
                )
        except mariadb.Error as e:
            raise Exception(f"Error connecting to MariaDB Platform: {e}")
            # sys.exit(1)

    def run_query(self, query:str):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor

    def close_conn(self):
         if self.conn != None:
            self.conn.close()

