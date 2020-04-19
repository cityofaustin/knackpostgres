import logging

import psycopg2

class Loader:
    """ Wrapper for executing Knack applicaton SQL commands """
    def __repr__(self):
        return f"<Loader {self.app.name}>"

    def __init__(self, app):
        self.app = app

    def connect(self, host="localhost", dbname="postgres", user="postgres", password=None):
        if not password:
            raise AttributeError("`password` is required to connect.")
        
        self.conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
        self.conn.autocommit = True
        return self.conn

    def create_tables(self):
        
        with self.conn.cursor() as cursor:
            for table in self.app.tables:
                print(table)
                cursor.execute(table.sql)
                logging.info(f"Created {table}")