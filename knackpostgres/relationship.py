from .constants import TAB


class Relationship:
    """
    A SQL translator for Knack `connection` fields <3
    """

    def __repr__(self):
        return f"<Relationship {self.name}>"

    def __init__(self, field, host_table=None, rel_table=None):

        if not field and host_table and rel_table:
            raise AttributeError(
                "Relationship requires `field`, `host_table`, and `rel_table`."
            )

        self.field = field
        self.host_table = host_table
        self.rel_table = rel_table
        self.type = self._type()
        self.child_table_name = self._child()
        self.parent_table_name = self._parent()
        self.host_field_key_knack = self.field.key_knack
        self.host_field_name = self.field.name_postgres
        self.name = self._name()
        self.sql = self._to_sql()

    def _type(self):
        if (
            self.field.relationship_knack["has"] == "one"
            and self.field.relationship_knack["belongs_to"] == "many"
        ):
            return "one_to_many"

        elif (
            self.field.relationship_knack["has"] == "many"
            and self.field.relationship_knack["belongs_to"] == "one"
        ):
            return "many_to_one"

        else:
            return "many_to_many"

    def _name(self):
        # this feels wronggg
        parent_relation = self.type.split("_")[0]
        child_relation = self.type.split("_")[2]
        return f"{parent_relation}_{self.parent_table_name}_to_{child_relation}_{self.child_table_name}"

    def _child(self):
        if self.type == "one_to_many":
            return self.host_table

        elif self.type in ["many_to_one", "many_to_many"]:
            return self.rel_table

        else:
            raise TypeError(f"Unknown relationship type: {self.type}")

    def _parent(self):
        if self.type == "one_to_many":
            return self.rel_table

        elif self.type in ["many_to_one", "many_to_many"]:
            return self.host_table

        else:
            raise TypeErrork(f"Unknown relationship type: {self.type}")

    def _relationship_field_name(self, parent_table_name):
        return f"{self.host_field_name}_{parent_table_name}_id"

    def _to_sql(self):
        if self.type in ["one_to_many", "many_to_one"]:
            return self._add_column_statement(self.parent, self.child_table_name)

        elif self.type == "many_to_many":
            sql_1 = self._add_column_statement(
                self.parent_table_name, self.child_table_name
            )
            sql_2 = self._add_column_statement(
                self.child_table_name, self.parent_table_name
            )
            return f"{sql_1}\n\n{sql_2}"

    def _add_column_statement(self, parent_table_name, child_table_name):
        rel_field_name = self._relationship_field_name(parent_table_name)
        return (
            f"""ALTER TABLE {child_table_name} ADD COLUMN {rel_field_name} NUMERIC;"""
        )
