from .field_def import FieldDef
from .utils import valid_pg_name
from .constants import TAB


class Table:
    """ A Knack table (`object`) wrapper """

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
        fields = [field.sql for field in self.fields if field.sql]

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
            "type": "_knack_id",  # todo: we'll have to map knack record "id" value to this field
        }

        return FieldDef(knack_id)

    def _generate_primary_key_field(self):

        pk = {
            "required": True,
            "unique": True,
            "name": "id",
            "key": "id",
            "type": "_pg_primary_key",  # todo: we'll have to map knack record IDs to this new serial
        }

        return FieldDef(pk, primary_key=True)
