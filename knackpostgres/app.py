"""
Convert a Knack application to a PostgreSQL Database.
"""
import logging
from pathlib import Path
from pprint import pprint as print
import pdb

from knackpy import get_app_data

from .constants import TAB
from .table import Table
from .relationship import Relationship
from .utils import valid_pg_name


class App:
    """
    Knack application wrapper. Stores app meta data, tables, fields, etc.
    
    Receives a Knack application ID string and returns App instance.

    On instanciation, will fetch app metadata and prepare output SQL statements.

    Usage:

    >>> `app = App("myappid")`
    >>> app.to_sql()   # write to /sql dir

    """

    def __repr__(self):
        return f"<App {self.name}> ({len(self.objects)} objects)"

    def __init__(self, app_id):

        self.app_id = app_id

        # fetch knack metadata
        self.data = self._get_app_data()

        # assign knack metadata to class attributes
        for key in self.data:
            setattr(self, key, self.data[key])

        self.tables = self._handle_tables()

        self.obj_lookup = self._generate_obj_lookup()

        self.relationships = self._create_relationships()

        # creates a "formula" key inside each <Field> with formula defs and sql
        self.tables = self._handle_standard_equations()

        logging.info(self)

    def to_sql(self, path="sql"):
        """
        Write application SQL commands to file.

        Alternatively, use the `Loader` class to connect/write directly
        from the `App` class.
        """
        for table in self.tables:
            self._write_sql(table.sql, path, "tables", table.name)

        for rel in self.relationships:
            self._write_sql(rel.sql, path, "relationships", rel.name)

    def _write_sql(self, sql, path, subdir, name_attr, method="w"):

        file_path = Path(path) / subdir

        file_path.mkdir(exist_ok=True, parents=True)

        file_path = file_path / f"{name_attr}.sql"

        with open(file_path, method) as fout:
            fout.write(sql)

    def _get_app_data(self):
        return get_app_data(self.app_id)

    def _handle_tables(self):
        return [Table(obj) for obj in self.objects]

    def _generate_obj_lookup(self):
        """ The obj_lookup allows us to find connected object keys across the entire app """
        return {table.key: table.name_postgres for table in self.tables}

    def _create_relationships(self):
        
        relationships = []

        for table in self.tables:

            for field in table.fields:

                if field.type_knack != "connection":
                    continue

                rel_obj = field.relationship_knack["object"]
                rel_table_name = self.obj_lookup[rel_obj]

                rel = Relationship(
                    field=field,
                    host_table=table.name_postgres,
                    rel_table=rel_table_name,
                )

                relationships.append(rel)

        return relationships

    def _standard_equation_to_sql(
        self, parent_table, child_table, child_field, field_name, method
    ):
        return f"""(SELECT {method}({child_field}) FROM {child_table} WHERE {child_table}.{parent_table}_id = {parent_table}.id) AS {field_name}"""

    def _handle_standard_equations(self):
        standard_eqs = ["count", "sum", "max", "average", "min"]

        for table in self.tables:

            for field in table.fields:
                if field.type_knack not in standard_eqs:
                    continue

                field.formula = {}

                field.formula["parent_table"] = table.name

                if field.type_knack == "count":
                    pg_method = "COUNT"

                    field.formula[
                        "child_table"
                    ] = self._find_table_name_from_connecton_field(
                        table, field.format_knack["connection"]
                    )

                    # for counts, always just count the primary key
                    field.formula["child_field"] = "id"

                else:
                    pg_method = field.type_knack.upper()
                    pg_method = "AVG" if pg_method == "AVERAGE" else pg_method

                    field.formula["child_table"] = self._find_table_name_from_field_key(
                        field.format_knack["field"]
                    )
                    field.formula["child_field"] = self._find_field_name_from_field_key(
                        field.format_knack["field"]
                    )

                field.formula["pg_method"] = pg_method
                field.formula["sql"] = self._standard_equation_to_sql(
                    field.formula.get("parent_table"),
                    field.formula.get("child_table"),
                    field.formula.get("child_field"),
                    field.name_postgres,
                    field.formula.get("pg_method"),
                )

        return self.tables

    def _find_table_name_from_connecton_field(self, table, key):
        # traverse relationships to get table name of a connnected field
        for rel in self.relationships:
            if rel.host_field_key_knack == key:
                # found it
                return rel.child

        return None

    def _find_field_name_from_field_key(self, key):
        """
        from a knack field key, track postgres fieldname
        """
        try:
            # some times the connection is under "key", and somtimes it's a string literal
            key = key.get("key")

        except AttributeError:
            pass

        for table in self.tables:
            for field in table.fields:
                if field.key_knack == key:
                    return field.name_postgres

        else:
            # field not found!
            return None

    def _find_table_name_from_field_key(self, key):
        """
        from a knack field key, track down the table in which that field lives
        """
        try:
            # some times the connection is under "key", and somtimes it's a string literal
            key = key.get("key")

        except AttributeError:
            pass

        for table in self.tables:
            for field in table.fields:
                if field.key_knack == key:
                    # we found it :)
                    return table.name

        # no table found that contains this key
        return None
