from knackpostgres.tables.table import Table
from knackpostgres.config.constants import PG_NULL

import pdb

class MetaTable(Table):
    """ Create a table in which to store app field metadata. Written to
    __meta__ schema in db """

    def __repr__(self):
        return f"<Table {self.name_postgres}> ({len(self.fields)} fields)"

    def __init__(self, app, schema):
        """ where app is an `App` class instance """

        self.schema = schema

        self.name_postgres = "fields"

        self.fields = self._set_fields()

        self.records = self._get_records(app)

    def _set_fields(self):
        return [
            {"name": "id", "constraints": " PRIMARY KEY", "type": "SERIAL"},
            {"name": "column_name", "constraints": "", "type": "TEXT"},
            {"name": "data_type", "constraints": "", "type": "TEXT"},
            {"name": "input_type", "constraints": "", "type": "TEXT"},
            {"name": "table_name", "constraints": "", "type": "TEXT"},
            {"name": "view_name", "constraints": "", "type": "TEXT"},
            {"name": "is_primary_key", "constraints": "", "type": "BOOLEAN"},
            {"name": "options", "constraints": "", "type": "TEXT[]"},
            {"name": "read_only", "constraints": "", "type": "BOOLEAN"},
            {"name": "is_formula", "constraints": "", "type": "BOOLEAN"},
            {"name": "is_connection", "constraints": "", "type": "BOOLEAN"},
        ]

    def _get_records(self, app):
        records = []

        for table in app.tables:
            for field in table.fields:
                records.append({
                    "column_name": field.name_postgres,
                    "data_type": field.data_type,
                    "input_type": self._handle_type(field),
                    "table_name": field.table.name_postgres,
                    "view_name": "not_implemented",
                    "is_primary_key" : field.primary_key,
                    "options": self._handle_options(field),
                    "is_formula": getattr(field, "is_formula", None),
                    "is_connection": getattr(field, "is_connection", None)
                })
        records = self._convert_nulls(records)
        return records

    def _convert_nulls(self, records):
        """ psycopg2 will write None as "None". So we convert NoneTypes to "NULL" """
        for record in records:
            for k, v in record.items():
                if v == None:
                    record[k] = PG_NULL
        return records

    def _handle_options(self, field):
        try:
            return field.format_knack.get("options")

        except AttributeError:
            return None

    def _handle_type(self, field):
        
        if field.type_knack == "multiple_choice":

            return field.format_knack.get("type")

        else:
            return field.type_knack

    def _field_to_sql(self, name, constraints, data_type):
        return f"{name} {data_type}{constraints}"

    def to_sql(self):
        sql = []

        fields_sql = [
            self._field_to_sql(field["name"], field["constraints"], field["type"])
            for field in self.fields
        ]
        all_fields_sql = f",\n    ".join(fields_sql)

        self.sql = f"""CREATE TABLE IF NOT EXISTS {self.schema}.{self.name_postgres} (\n    {all_fields_sql}\n);\n\n"""
        return self.sql
