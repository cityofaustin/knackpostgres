from pprint import pprint as print
from knackpostgres.tables._table import Table
from knackpostgres.fields.meta_field import MetaField
from knackpostgres.config.constants import PG_NULL
from knackpostgres.config.metadata import METADATA_FIELDS


class MetaTable(Table):
    """ Create a table in which to store app field metadata. Written to
    __meta__ schema in db """

    def __init__(self, data, name, schema):
        """ where app is an `App` class instance """
        super().__init__(data, name, schema)
        self.fields += self._get_fields()
        self.rows = self._get_rows(data)

    def _get_fields(self):
        return [MetaField(field, field["name"], self) for field in METADATA_FIELDS.get(self.name_postgres)]

    def _get_row(self, field):
        # todo: so...need to refactor for this nonsense with metadata fields
        row = {}
        
        for metafield in self.fields:
            name = metafield.name_postgres
            accessor = metafield.accessor

            if not accessor:
                # skip, for excample, the tables primary key field
                continue

            val = None

            try:
                # assume accessor is a field property
                val = getattr(field, accessor)

            except AttributeError:
                pass

            try:
                # try to access property at field.abc.xyz
                accessors = metafield.accessor.split(".")
                subclass = getattr(field, accessors[0])
                val = getattr(field, accessors[1])

            except (IndexError, AttributeError):
                pass

            try:
                # ok, accessor is not a field property, so try handler
                handler = getattr(self, accessor)
                val = handler(field)

            except Exception as e:
                pass

            row[name] = val

        return row

    def _get_rows(self, data):
        rows = []

        for field in data:
            row = self._get_row(field)
            rows.append(row)
           
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
        else:
            return field.type_knack

    def to_sql(self):
        sql = []

        fields_sql = [field.to_sql() for field in self.fields]

        all_fields_sql = f",\n    ".join(fields_sql)

        self.sql = f"""CREATE TABLE IF NOT EXISTS {self.schema}.{self.name_postgres} (\n    {all_fields_sql}\n);\n\n"""
        return self.sql
