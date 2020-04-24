import csv
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

    def insert_each(self, translator):
        with self.conn.cursor() as cursor:
            for statement in translator.sql:
                self._execute_sql(cursor, statement, translator.table.name_postgres)

    def _execute_sql(self, cursor, sql, name):
        try:
            cursor.execute(sql)
            logging.info(f"Created {name}")

        except psycopg2.ProgrammingError as e:
            logging.error(f"Failed to create {name}")
            logging.error(e)
            pass

    def load_csv(self, translator):
        table_name = translator.table.name_postgres
        # records = json.loads(json.dumps(translator.records))
        # sql_string = 'INSERT INTO {} '.format( table_name )
        # columns = translator.fieldnames
        # values = []
        # for record in records:
        #     for k, v in record.items:
        #         val = "'" + val + "'"
        # import pdb; pdb.set_trace()
        # import json
        # import io


        # with self.conn.cursor() as cursor:
        #     for record in translator.records:
        #         json_to_write = json.dumps(record).replace('\\','\\\\')
        #         buffer_ = io.StringIO(json_to_write)                
        #         cursor.copy_from(buffer_, table_name, columns=translator.fieldnames, sep=",")
        #         pdb.set_trace()
        # pdb.set_trace()

        with open(translator.fname, "r") as fin:
            with self.conn.cursor() as cursor:
                cursor.copy_from(fin, table_name, columns=translator.fieldnames, sep="|")
