from knackpostgres.tables._table import Table
from knackpostgres.fields.meta_field import MetaField
from knackpostgres.config.constants import FIELD_DEFINITIONS, PG_NULL

METADATA_FIELDS = [
    {"name": "column_name", "type": "TEXT"},
    {"name": "data_type", "type": "TEXT"},
    {"name": "input_type", "type": "TEXT"},
    {"name": "table_name", "type": "TEXT"},
    {"name": "view_name", "type": "TEXT"},
    {"name": "is_primary_key", "type": "BOOLEAN"},
    {"name": "options", "type": "TEXT[]"},
    {"name": "read_only", "type": "BOOLEAN"},
    {"name": "is_formula", "type": "BOOLEAN"},
    {"name": "is_connection", "type": "BOOLEAN"},
]


class MetaTable(Table):
    """ Create a table in which to store app field metadata. Written to
    __meta__ schema in db """

    def __init__(self, data, name, schema):
        """ where app is an `App` class instance """
        super().__init__(data, name, schema)
        self.fields += self._get_fields()
        self.rows = self._get_rows(data)

    def _get_fields(self):
        return [MetaField(field, field["name"], self) for field in METADATA_FIELDS]

    def _get_rows(self, data):
        rows = []

        for field in data:
            rows.append(
                {
                    "column_name": field.name_postgres,
                    "data_type": field.data_type,
                    "input_type": self._handle_type(field),
                    "table_name": field.table.name_postgres,
                    "view_name": "not_implemented",
                    "is_primary_key": field.is_primary_key,
                    "options": self._handle_options(field),
                    "is_formula": getattr(field, "is_formula", None),
                    "is_connection": getattr(field, "is_connection", None),
                }
            )
        rows = self._convert_nulls(rows)
        return rows

    def _convert_nulls(self, rows):
        """ psycopg2 will write None as "None". So we convert NoneTypes to "NULL" """
        for row in rows:
            for k, v in row.items():
                if v == None:
                    row[k] = PG_NULL
        return rows

    def _handle_options(self, field):
        try:
            return field.format_knack.get("options")

        except AttributeError:
            return None

    def _handle_type(self, field):
        if field.is_primary_key:
            return "_pg_primary_key"

        elif field.type_knack == "multiple_choice":
            return field.format_knack.get("type")

        else:
            return field.type_knack

    def to_sql(self):
        sql = []

        fields_sql = [field.to_sql() for field in self.fields]

        all_fields_sql = f",\n    ".join(fields_sql)

        self.sql = f"""CREATE TABLE IF NOT EXISTS {self.schema}.{self.name_postgres} (\n    {all_fields_sql}\n);\n\n"""
        return self.sql
