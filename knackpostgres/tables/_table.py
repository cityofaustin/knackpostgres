from knackpostgres.utils.utils import valid_pg_name
from knackpostgres.fields.meta_field import MetaField


class Table:
    """ A Knack table (`object`) wrapper """

    def __repr__(self):
        return f"<Table {self.name_postgres}> ({len(self.fields)} fields)"

    def __init__(self, data, name, schema):
        self.schema = schema
        self.name_postgres = valid_pg_name(name)

        # Note that all children of `Table` are initialzied with a primary key field like so
        self.fields = []
        self.fields.append(self._primary_key_field())

    def _primary_key_field(self):
        field_data = {
            "required": True,
            "unique": True,
            "name": "id",
            "key": "id",
            "type": "SERIAL",
        }
        return MetaField(field_data, field_data["name"], self)
