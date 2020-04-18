"""
Convert a Knack application to a PostgreSQL Database.
"""
import logging
from pathlib import Path
from pprint import pprint as print
import pdb

from knackpy import get_app_data

from .constants import EXCLUDED_TYPES, TAB
from .utils import *


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

        self.relationships = self._handle_relationships()

        self.sql_relationships = self._generate_relationship_sql()

        # creates a "formula" key inside each <Field> with formula defs and sql
        self.tables = self._handle_standard_equations()

        print(self)

    def to_sql(self, path="sql"):

        for table in self.tables:
            self._write_sql(table.sql, path, "tables", table.name)

        for rel in self.sql_relationships:
            self._write_sql(rel["sql"], path, "relationships", rel["name"])

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

    def _generate_relationship_sql(self):
        statements = []

        for rel in self.relationships:
            statement = {"name": rel["name"]}

            if rel["type"] == "one_to_many":
                sql = self._one_to_many_statement(rel)
                statement.update({"sql": sql})

            elif rel["type"] == "many_to_many":
                sql = self._many_to_many_statement(rel)
                statement.update({"sql": sql})

            statements.append(statement)

        return statements

    def _one_to_many_statement(self, rel):
        return f"""ALTER TABLE {rel["child"]}\nADD CONSTRAINT {rel["parent_field_name"]} FOREIGN KEY (id) REFERENCES {rel["parent"]} (id);\n\n"""

    def _many_to_many_statement(self, rel):
        # see https://stackoverflow.com/questions/9789736/how-to-implement-a-many-to-many-relationship-in-postgresql
        t1 = rel["parent"]
        t2 = rel["child"]
        pk1 = f"{t1}_id"
        pk2 = f"{t2}_id"
        rel_table_name = f"{t1}_{t2}"

        return f"""CREATE TABLE IF NOT EXISTS {rel_table_name} (\n{TAB}{pk1} integer REFERENCES {t1} (id) ON UPDATE CASCADE ON DELETE CASCADE,\n{TAB}{pk2} integer REFERENCES {t2} (id) ON UPDATE CASCADE,\n{TAB}CONSTRAINT {rel_table_name}_pk PRIMARY KEY ({pk1}, {pk2})\n);\n\n"""

    def _handle_relationships(self):
        """
        >> one to many

        'object_53 cameras signal'
        "{'object': 'signals', 'has': 'one', 'belongs_to': 'many'}"
        so object is related object. has one belongs to many means current table is the child
        
        one signal has many cameras

        >> many to many
        'object_75 tmc_activities signals_affected'
        "{'belongs_to': 'many', 'has': 'many', 'object': 'signals'}"
        many signals have mand tmc_activities

        >> many to one
        'object_118 signals_cabinet signal'
        "{'belongs_to': 'one', 'has': 'many', 'object': 'signals'}"

        one cabinet has many signals at cabinet_id
        """

        app_relationships = []

        obj_lookup = {table.key: table.name_postgres for table in self.tables}

        for table in self.tables:
            connection_fields = [
                field for field in table.fields if field.type_knack == "connection"
            ]

            if not connection_fields:
                continue

            for conn_field in connection_fields:
                rel_obj = conn_field.relationship_knack["object"]
                rel_table_name = obj_lookup[rel_obj]
                rel_name = f"{table.name_postgres}_{rel_table_name}"

                rel = {"name": rel_name}

                if (
                    conn_field.relationship_knack["has"] == "one"
                    and conn_field.relationship_knack["belongs_to"] == "many"
                ):
                    # one-to-many relationship
                    rel.update(
                        {
                            "field_key_knack": conn_field.key_knack,
                            "parent_field_name": f"{rel_table_name}_id",
                            "child": table.name_postgres,
                            "parent": rel_table_name,
                            "type": "one_to_many",  # one parent to many children
                        }
                    )

                elif (
                    conn_field.relationship_knack["has"] == "many"
                    and conn_field.relationship_knack["belongs_to"] == "one"
                ):
                    # many-to-one relationship
                    # this is an artefact of knack relationships.
                    # in postgres one-to-many and many-to-one are the same (i think...todo)
                    rel.update(
                        {
                            "field_key_knack": conn_field.key_knack,
                            "parent_field_name": f"{table.name_postgres}_id",
                            "child": rel_table_name,
                            "parent": table.name_postgres,
                            "type": "one_to_many",  # one parent to many children
                        }
                    )

                else:
                    # many-to-many
                    # see: https://stackoverflow.com/questions/9789736/how-to-implement-a-many-to-many-relationship-in-postgresql
                    rel.update(
                        {
                            "field_key_knack": conn_field.key_knack,
                            "child": rel_table_name,
                            "parent": table.name_postgres,  # the connection field exists on this object. this is important to get right for purposes of formulas
                            "type": "many_to_many",  # many parents to many children
                        }
                    )

                app_relationships.append(rel)

        return app_relationships

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
            if rel["field_key_knack"] == key:
                # found it
                return rel["child"]

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


class Table:
    def __repr__(self):
        return f"<Table {self.name_postgres}> ({len(self.fields)} fields)"

    def __init__(self, data):
        # where data is knack "objects" list from app data

        for key in data:
            setattr(self, key, data[key])

        self.name_postgres, self.name_knack = valid_pg_name(self.name)

        self.fields = self._handle_fields()

        # drop connections, formulae?
        self.field_map = self._generate_field_map()

        self.sql = self.to_sql()

    def _handle_fields(self):

        # adds an "obj_name" key to field Class, which comes in useful for debugging
        fields = [
            field.update({"obj_name": self.name_postgres})
            for field in self.fields
            if field["type"]
        ]

        fields = [FieldDef(field) for field in self.fields]

        fields.append(self._generate_primary_key_field())

        fields.append(self._generate_knack_id_field())

        return fields

    def to_sql(self):
        fields = [
            field.sql for field in self.fields if field.type_knack not in EXCLUDED_TYPES
        ]
        field_sql = f",\n{TAB}".join(fields)

        return f"""CREATE TABLE IF NOT EXISTS {self.name_postgres} (\n{TAB}{field_sql}\n);\n\n"""

    def _generate_field_map(self):
        return {
            field.key_knack: {"name": field.name_postgres, "type": field.type_knack}
            for field in self.fields
        }

    def _generate_knack_id_field(self):
        knack_id = {
            "required": True,
            "unique": True,
            "name": "knack_id",
            "key": "knack_id",
            "type": "short_text",  # todo: we'll have to map knack record "id" value to this field
        }

        return FieldDef(knack_id)

    def _generate_primary_key_field(self):
        # we're setting all primary keys to the knack record id
        pk = {
            "required": True,
            "unique": True,
            "name": "id",
            "key": "id",
            "type": "pg_primary_key",  # todo: we'll have to map knack record IDs to this new serial
            "primary_key": True,
        }

        return FieldDef(pk)


class FieldDef:
    def __repr__(self):
        return f"<FieldDef {self.name_postgres}> ({self.type_knack})"

    def __init__(self, data):
        # where data is knack "fields" list from app data

        self.primary_key = False
        self.constraints = None
        self.default_knack = None

        for key in data:
            setattr(self, key + "_knack", data[key])

        # todo: remove this temporarry datatype setter
        self.data_type = "text"

        # convert field name to underscore and presever original
        self.name_postgres, self.name_knack = valid_pg_name(self.name_knack)

        self.default_postgres = self._set_default()

        self.constraints = self._get_constraints()

        if self.type_knack in ["short_text", "pargraph_text", "phone", "link"]:
            self.data_type = "TEXT"

        elif self.type_knack == "multiple_choice":
            self._handle_multi_choice()

        elif self.type_knack == "date_time":
            self._handle_date()

        elif self.type_knack in ["number", "currency"]:
            self.data_type = "NUMERIC"

        elif self.type_knack == "connection":
            self.data_type = "will_be_dropped"

        elif self.type_knack in ["address", "name"]:
            self.data_type = "JSON"

        elif self.type_knack == "pg_primary_key":
            self.data_type = "SERIAL"

        elif self.type_knack == "boolean":
            self.data_type = "BOOLEAN"

        self._handle_rules()

        self.sql = self._to_sql()

    def _set_default(self):

        if not hasattr(self, "default_knack"):
            return None

        elif (
            self.default_knack == "kn-blank"
            or self.default_knack == ""
            or self.default_knack == None
        ):
            return None

        else:
            try:
                # handle string-quoted number defaults
                return int(self.default_knack)
            except ValueError:
                return self.default_knack

    def _handle_rules(self):
        # SOMEDAY!
        # if hasattr(self, "rules"):
        #     if self.rules:
        #         print(self.rules)
        return None

    def _get_constraints(self):
        constraints = []

        if self.required_knack:
            constraints.append("NOT NULL")

        if self.unique_knack:
            constraints.append("UNIQUE")

        return constraints

    def _format_default(self):

        # i must be missing something, but an inline if was not properly handling an empty string string
        if self.default_postgres:
            default = self.default_postgres
        else:
            default = ""

        try:
            # just in case a default val contains a single quote
            default = default.replace("'", "\\'")
        except AttributeError:
            pass

        default = str(default).upper() if type(default) == bool else f"'{default}'"

        return f"DEFAULT {default} "

    def _to_sql(self):

        pk = "PRIMARY KEY" if self.primary_key else ""

        default = self._format_default()

        constraints = " ".join(self.constraints) if self.constraints else ""

        return (
            f"{pk} {self.name_postgres} {self.data_type} {default}{constraints}".strip()
        )

    def _handle_date(self):
        date_format = self.format_knack["date_format"].lower()
        time_format = self.format_knack["time_format"].lower()

        if "ignore" in date_format:
            self.data_type = "TIME WITH TIME ZONE"
        else:
            self.data_type = "TIMESTAMP WITH TIME ZONE"

    def _handle_multi_choice(self):

        self.data_type = "text"  # postgres type

        try:
            # none type options are ["single", "radios", "multi"]
            if self.format_knack["type"] == "multi":
                self.array = True

        except KeyError:
            # if no "type", this is a knack user role field
            # so just continue for now
            # todo: look at this
            pass

        # todo handle format elsewhere
        self.options = self.format_knack.get("options")

        self.blank = self.format_knack.get("blank")

        self.sorting = self.format_knack.get("sorting")


def connect():
    conn_string = "host='localhost' dbname='postgres' user='postgres' password='pizza'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    with open("data/signals.csv", "r") as fin:
        reader = csv.reader(fin)
        fieldnames = next(reader)
        cursor.copy_from(fin, "signals", columns=fieldnames, sep=",")
        conn.commit()
        conn.close()
