import logging

import psycopg2


class Loader:
    """ Wrapper for executing Knack applicaton SQL commands """

    def __repr__(self):
        return f"<Loader {self.app.name}>"

    def __init__(self, app, overwrite=False):
        self.app = app

        # if true, will drop entire public schema from dest database!
        self.overwrite = overwrite

    def connect(
        self, host="localhost", dbname="postgres", user="postgres", password=None
    ):
        if not password:
            raise AttributeError("`password` is required to connect.")

        self.conn = psycopg2.connect(
            host=host, dbname=dbname, user=user, password=password
        )
        self.conn.autocommit = True
        return self.conn

    def create_tables(self):

        with self.conn.cursor() as cursor:
            if self.overwrite:
                cursor.execute("DROP SCHEMA public CASCADE;")
                cursor.execute("CREATE SCHEMA public;")

            for table in self.app.tables:
                self._execute_sql(cursor, table.sql, table.name)

    def create_views(self):
        with self.conn.cursor() as cursor:
            for view in self.app.views:
                self._execute_sql(cursor, view.sql, view.name)

    def _execute_sql(self, cursor, sql, name):
        try:
            cursor.execute(sql)
            logging.info(f"Created {name}")

        except psycopg2.ProgrammingError as e:
            logging.error(f"Failed to create {name}")
            logging.error(e)
            pass
