"""
Convert a Knack application to a PostgreSQL Database.
"""
import csv
import json
from pathlib import Path
from pprint import pprint as print
import pdb

import knackpy
import psycopg2

# knack "password" fields are of type 'short_text' in metadata
# we need to exclude them by checking the field name 'password'
# note that any user-defined field of this name would be skipped. todo! address this!
# these are handled elsewhere (connections and formulae) or useless internal knack fields
EXCLUDED_TYPES = ["password", "max", "min", "count", "average", "concatenation", "connection"]

TAB = "    "


def valid_pg_name(original_name):
    """
    Convert an arbitrary string into a postgres-compliant name.
    Try not too make it too ugly while you're at it.

    Warning, this opens up the possiblity that input `original_name` will
    no longer be unique to it's class instance.

    E.g., names `2-A` and `2$A` both resolve to `_2_A`.

    Best practice: label your knack fields and objects with postgres-compliant names.

    Docs: https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html
    """

    # first, make lowercase and replace spaces with underscores
    new_name = original_name.lower().replace(" ", "_")

    # first character cannot be a number. if so, put underscore in front of it
    new_name = new_name if not new_name[0].isdigit() else "_" + new_name

    # replace non-alphanum chars with underscore
    new_name = "".join(e if e.isalnum() else "_" for e in new_name)

    return new_name, original_name



class Translator:
    def __repr__(self):
        return f"<Translator {self.knack.obj}> to {self.table.name}"

    def __init__(self, knack, table):
        # where knack is a knackpy.Knack object and table is a Table class instance
        self.knack = knack
        self.table = table
        self.knack.data_raw = self._replace_raw_fieldnames()
        self.records = self._translate_records()
        self.records = self._convert_fieldnames()
    

    def _drop_forbidden_fields(self):

        return [
            field
            for field in knack.data_raw
            if field["name"].lower() not in FORBIDDEN_FIELD_NAMES
        ]

    def _drop_excluded_fields(self):
        
        new_records = []

        for record in self.knack.data_raw:
            new_record = {}

            for key in record.keys():
                try:
                    field_type = self.knack.fields[key].get("type")
                except KeyError:
                    # also dropping "account_status" and any other internal fields. TODO: warning
                    continue

                if field_type in EXCLUDED_TYPES:
                    print(field_type)
                    continue
                else:
                    new_record[key] = record[key]

            new_records.append(new_record)

        return new_records

    def _translate_records(self):
        translated_records = []

        for record in self.knack.data_raw:
            translated_record = {}

            for field in record.keys():

                try:
                    field_type = self.knack.fields.get(field).get("type")

                except AttributeError:
                    # knack internal fields are not exposed in field metadata
                    # which is fine, we skip them
                    continue

                # use the Handler to translate the data based on field type
                try:
                    handler = Handler(field_type)
                except ValueError:
                    # a handler has been explicitly NOT defined for this field type
                    # field will be dropped
                    continue

                
                try:
                    translated_record[field] = handler.handle(record[field])

                except ValueError:
                    print(f"skipping empty string: {field_type}")
                    continue

            translated_records.append(translated_record)

        return translated_records

    def _replace_raw_fieldnames(self):
        """
        For any Knack field that has both a "raw" field, use the raw field and drop the
        non-raw field
        """

        # sample the first record to get all fieldnames
        record = self.knack.data_raw[0]

        raw_fields = [
            field.split("_raw")[0] for field in record.keys() if field.endswith("_raw")
        ]

        new_records = []

        for record in self.knack.data_raw:

            new_record = {}

            for field in record.keys():

                # lookup the destination postgres fieldname and replace keys accordingly
                if field == "id":
                    new_field = field

                elif field.endswith("_raw"):
                    new_field = field.split("_raw")[0]

                elif field not in raw_fields:
                    # this field does not have a raw field, so use this one
                    new_field = field

                else:
                    # ignore the field, assuming it's a dupe of the "raw" field
                    continue

                new_record[new_field] = record[field]

            new_records.append(new_record)

        return new_records

    def _convert_fieldnames(self):
        """
        Lookup the destination postgres fieldname and replace keys accordingly
        """
        new_records = []

        for record in self.records:
            new_record = {}

            for field in record.keys():

                if field == "profile_keys_raw":
                    # ignore this knack meta field
                    # todo: move elsewher
                    continue

                if field == "id":
                    # our App class expects knack ids to be represented with a "knack_id" fieldname
                    converted_field_name = "knack_id"

                else:
                    try:
                        converted_field_name = self.table.field_map.get(field).get(
                            "name"
                        )
                    except AttributeError:
                        # field was not defined. probably a passwordfield
                        print(
                            f"Warning: {self.knack.fields[field]['label']} ({field}) is not defined and will be ignored."
                        )
                        continue

                new_record[converted_field_name] = record[field]

            new_records.append(new_record)

        return new_records

    
    def to_csv(self, path="data"):
            
        path = Path.cwd() / path
        path.mkdir(exist_ok=True)
        fname = path / (self.table.name + ".csv")
        
        with open(fname, "w") as fout:

            fieldnames = [field for field in self.records[0].keys()]

            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for record in self.records:
                writer.writerow(record)

        print(f"{fname} - {len(self.records)} records")


class Handler:
    def __repr__(self):
        return f"<Handler type=`{self.type}` name=`{self.handler.__name__}`>"

    def __init__(self, field_type):

        if field_type in EXCLUDED_TYPES:
            raise ValueError(f"Forbidden field type: {field_type}")

        self.type = field_type
        
        try:
            self.handler = getattr(self, "_" + self.type + "_handler")
        except AttributeError:
            self.handler = getattr(self, "_default_handler")

    def handle(self, val):
        
        if val == "":
            # todo: this could vary by handler
            return None

        return self.handler(val)

    def _link_handler(self, val):
        return val.get("url")

    def _default_handler(self, val):
        """
        Handles these fieldtypes:
            auto_increment
            paragraph_text
            phone
            multiple_choice
            currency
            short_text
            name
            number
            boolean
        """
        return val

    def _file_handler(self, val):
        return val.get("url")

    def _image_handler(self, val):
        # image will be a url or key/val pair 
        try:
            return val.get("url")

        except AttributeError:
            return val.strip() # noticed some leading white space in data tracker

    def _date_time_handler(self, val):
        return val.get("iso_timestamp")

    def _email_handler(self, val):
        return val.get("email")


class App:
    """
    Knack application wrapper. Stores app meta data, tables, fields, etc.
    
    Input is a Knack application ID. Outputs to "app.sql", a series of postgresql statements which
    create tables, relationships etc.
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
        
        path = Path.cwd() / path
        path.mkdir(exist_ok=True)

        with open(path / "tables.sql", "w") as fout:
            for table in self.tables:
                fout.write(table.sql)
                print(table)

        with open(path / "relationships.sql", "w") as fout:
            for statement in self.sql_relationships:
                fout.write(statement)
            print(f"{len(self.sql_relationships)} relationships written.")

        return None


    def _get_app_data(self):
        return knackpy.get_app_data(self.app_id)

    def _handle_tables(self):
        return [Table(obj) for obj in self.objects]

    def _generate_relationship_sql(self):
        statements = []

        for rel in self.relationships:
            if rel["type"] == "one_to_many":
                sql = self._one_to_many_statement(rel)
                statements.append(sql)

            elif rel["type"] == "many_to_many":
                sql = self._many_to_many_statement(rel)
                statements.append(sql)

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

                if (
                    conn_field.relationship_knack["has"] == "one"
                    and conn_field.relationship_knack["belongs_to"] == "many"
                ):
                    # one-to-many relationship
                    app_relationships.append(
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
                    app_relationships.append(
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
                    app_relationships.append(
                        {
                            "field_key_knack": conn_field.key_knack,
                            "child": rel_table_name,
                            "parent": table.name_postgres,  # the connection field exists on this object. this is important to get right for purposes of formulas
                            "type": "many_to_many",  # many parents to many children
                        }
                    )

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
        fields = [field.sql for field in self.fields if field.type_knack not in EXCLUDED_TYPES]
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

        elif self.default_knack == "kn-blank" or self.default_knack == "" or self.default_knack == None:
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
            pdb.set_trace()
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
    
    
    with open('data/signals.csv', "r") as fin:
        reader = csv.reader(fin)
        fieldnames = next(reader)
        cursor.copy_from(fin, 'signals', columns=fieldnames, sep=",")
        conn.commit()
        conn.close()
