import csv
import logging
import sys

import psycopg2


class Loader:
    """ Wrapper for executing Knack applicaton SQL commands """

    def __repr__(self):
        return f"<Loader {self.app.name}>"

    def __init__(self, app, overwrite=False):
        self.app = app

        # if true, will drop entire public schema from dest database!
        self.overwrite = overwrite

        # connection sql must be provided by Translator class (see README)
        self.connections_sql = []

    def connect(
        self,
        host="localhost",
        dbname="postgres",
        user="postgres",
        password=None,
        port=5432,
    ):
        if not password:
            raise AttributeError("`password` is required to connect.")

        self.dbname = dbname

        self.conn = psycopg2.connect(
            host=host, dbname=dbname, user=user, password=password, port=port
        )
        self.conn.autocommit = True
        self._confirm_overwrite()
        return None

    def _confirm_overwrite(self):
        if not self.overwrite:
            return None

        print(
            f"\n!! You are about to overwrite the `{self.app.schema}` and `{self.app.metadata_schema}` schema in the destination database !!"
        )
        confirmed = input('Type "yes"` to continue.\n')

        if confirmed.lower() != "yes":
            print("Loader aborted.")
            sys.exit()

        self._drop_destination_schema()

    def _drop_destination_schema(self):
        schema = []
        self.execute(f"DROP SCHEMA {self.app.schema} CASCADE;")
        self.execute(f"DROP SCHEMA {self.app.metadata_schema} CASCADE;")

    def create_schema(self):
        self.execute(self.app.schema_sql)
        self.execute(
            f"ALTER DATABASE {self.dbname} SET search_path TO {self.app.schema},'public';"
        )

    def create_tables(self):
        for table in self.app.metadata:
            self.execute(table.sql)

        for table in self.app.tables:
            self.execute(table.sql)

    def _sequence_views(self):
        """
        Some views depend on fields in other views. We sort
        the views to ensure that each view is created after
        its dependencies.

        TODO: what about co-dependent views? yikes.
        """
        sequenced_view_names = []

        # first, we generate a list of view names in order
        # using the `depends_on` attribute of each view
        for i, view in enumerate(self.app.views):
            if view.name in sequenced_view_names:
                continue

            if not view.depends_on:
                sequenced_view_names.insert(0, view.name)
                continue

            for dependency_view in view.depends_on:
                if dependency_view in sequenced_view_names:
                    continue
                sequenced_view_names.append(dependency_view)

            sequenced_view_names.append(view.name)

        # now we re-order the actual view classes in the app
        sequenced_views = []
        for view_name in sequenced_view_names:
            for view in self.app.views:
                if view.name == view_name:
                    sequenced_views.append(view)

        return sequenced_views

    def create_views(self):
        self.app.views = self._sequence_views()

        for view in self.app.views:
            self.execute(view.sql)

    def update_connections(self):
        for sql in self.connections_sql:
            self.execute(sql)

    def execute(self, sql):
        """ executes a singal sql statement or a list of them """
        with self.conn.cursor() as cursor:
            try:
                self._execute_one(cursor, sql)

            except TypeError:
                self._execute_many(cursor, sql)

            except psycopg2.ProgrammingError as e:
                logging.error(e)
                pass

    def _execute_one(self, cursor, sql):
        return cursor.execute(sql)

    def _execute_many(self, cursor, sql_list):
        for sql in sql_list:
            cursor.execute(sql)

        return None
