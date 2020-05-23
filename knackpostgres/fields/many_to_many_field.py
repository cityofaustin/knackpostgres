from .connection_field import ConnField
from knackpostgres.config.constants import FIELD_DEFINITIONS


class ManyToManyField(ConnField):
    """ Attribute setter for many-to-many connection fields """
    
    def __init__(self, data, table):
        super().__init__(data, table)

    def _construct_field_data(self, key, name, type_knack, required=True, unique=False):
        return {
            "key": key,
            "name": name,
            "required": required,
            "unique": unique,
            "type": type_knack,
        }

    def set_relationship_references(self, app):
        self.host_table_name = self.table.name_postgres

        self.rel_table_key = self.relationship_knack["object"]

        self.rel_table = app.find_table_from_object_key(
            self.rel_table_key
        )

        self.rel_table_name = self.rel_table.name_postgres

        self.reference_table_name = f"many_{self.host_table_name}_to_many_{self.rel_table_name}"

        host_table_reference_field_data = self._construct_field_data(
            f"{self.host_table_name}_id", f"{self.host_table_name}_id", "number"
        )

        rel_table_reference_field_data = self._construct_field_data(
            f"{self.rel_table_name}_id", f"{self.rel_table_name}_id", "number"
        )

        self.reference_table_data = {
            "key": self.reference_table_name,
            "name": self.reference_table_name,
            "fields": [host_table_reference_field_data, rel_table_reference_field_data],
        }

        return self
