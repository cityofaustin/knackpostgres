from knackpostgres.fields.field_def import FieldDef
from knackpostgres.fields.concatenation_field import ConcatenationField
from knackpostgres.fields.formula_field import FormulaField
from knackpostgres.fields.many_to_one_field import ManyToOneField
from knackpostgres.fields.many_to_many_field import ManyToManyField
from knackpostgres.fields.standard_field import StandardField
from knackpostgres.utils.utils import valid_pg_name
from knackpostgres.config.constants import FIELD_DEFINITIONS, TAB


class Table:
    """ A Knack table (`object`) wrapper """

    def __repr__(self):
        return f"<Table {self.name_postgres}> ({len(self.fields)} fields)"

    def __init__(self, data, schema):
        # where data is knack "objects" list from app data

        for key in data:
            setattr(self, key, data[key])

        self.schema = schema

        self.name_postgres, self.name_knack = valid_pg_name(self.name)

        self.fields = self._handle_fields()

        

    def update_one_to_many_relationships(self, obj_lookup):

        for field in self.fields:
            
            try:
                if field.relationship_type == "many_to_many":
                    continue
            
            except AttributeError:
                continue
            
            rel_obj = field.relationship_knack["object"]
            rel_table_name = obj_lookup[rel_obj]
            
            field.handle_relationship(
                host_table_name=self.name_postgres, rel_table_name=rel_table_name
            )

        return self

    def _remove_dupes(self):
        # sometimes the metadata has duplicate, identical entries for the same field.
        # they have different knack record IDs, so....?
        field_keys = []
        cleaned = []

        for field in self.fields:
            if field["key"] not in field_keys:
                field_keys.append(field["key"])
                cleaned.append(field)

        return cleaned

    def _handle_fields(self):
        self.fields = self._remove_dupes()
        
        self.fields.append(self._primary_key_field())
        
        self.fields.append(self._knack_id_field())

        classified = self._classify_fields()

        concats = [ConcatenationField(field, self) for field in classified["concats"] ]

        formula_fields = [FormulaField(field, self) for field in classified["formulas"] ]

        many_to_one_fields = [ManyToOneField(field, self) for field in classified["connections"]["many_to_one"]]

        many_to_many_fields = [ManyToManyField(field, self) for field in classified["connections"]["many_to_many"]]

        standard_fields = [StandardField(field, self) for field in classified["standard"]]

        return concats + formula_fields + many_to_one_fields + many_to_many_fields + standard_fields

    def _classify_fields(self):
        fields = {
            "concats" : [],
            "formulas": [],
            "connections": {
                "many_to_one": [],
                "many_to_many": []
            },
            "standard": []
        }

        for field in self.fields:
            if field["type"] == "concatenation":
                fields["concats"].append(field)

            elif self._is_formula(field):
                fields["formulas"].append(field)

            elif self._is_connection(field):
                has = field["relationship"]["has"]
                belongs_to = field["relationship"]["belongs_to"]
                connection_type = f"{has}_to_{belongs_to}"

                try:
                    fields["connections"][connection_type].append(field)
                except KeyError:
                    # assume one-to-many field (handled the same as many-to-one)
                    fields["connections"]["many_to_one"].append(field) 

            else:
                fields["standard"].append(field)

        return fields

    def _is_connection(self, field):
        return True if field["type"] == "connection" else False

    def _is_formula(self, field):
        if FIELD_DEFINITIONS.get(field["type"]).get("is_formula"):
            return True
        else:
            return False

    def to_sql(self):

        fields_sql = [
            field.to_sql()
            for field in self.fields
            if not type(field) in [ConcatenationField, FormulaField, ManyToManyField]
        ]

        fields_sql = f",\n{TAB}".join(fields_sql)

        self.sql = f"""CREATE TABLE IF NOT EXISTS {self.name_postgres} (\n{TAB}{fields_sql}\n);\n\n"""
        return self.sql

    def create_field_map(self):
        self.field_map = {
            field.key_knack: {"name": field.name_postgres, "type": field.type_knack}
            for field in self.fields
        }
        return None

    def _knack_id_field(self):
        return {
            "required": False,
            "unique": True,
            "name": "knack_id",
            "key": "knack_id",
            "type": "_knack_id",  # todo: we'll have to map knack record "id" value to this field
        }

        return FieldDef(knack_id_params, self)

    def _primary_key_field(self):

        return {
            "required": True,
            "unique": True,
            "name": "id",
            "key": "id",
            "type": "_pg_primary_key",  # todo: we'll have to map knack record IDs to this new serial
        }
