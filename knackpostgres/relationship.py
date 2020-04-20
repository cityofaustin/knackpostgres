from .constants import TAB

class Relationship:
    """
    Create table relationships from Knack `connection` fields <3
    """

    def __repr__(self):
        return f"<Relationship {self.name}>"

    def __init__(self, field=None, host_table=None, rel_table=None):

        if not field and host_table and rel_table:
            raise AttributeError(
                "Relationship requires `field`, `host_table`, and `rel_table`."
            )

        self.field = field
        self.host_table = host_table
        self.rel_table = rel_table

        self.type = self._type()
        self.name = self._name()
        self.child = self._child()
        self.parent = self._parent()
        self.parent_field_name = f"{self.parent}_id"
        self.host_field_key_knack = self.field.key_knack
        self.sql = self._sql()

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
        pre = self.type.split("_")[0]
        post = self.type.split("_")[2]
        return f"{pre}_{self.host_table}_to_{post}_{self.rel_table}"

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
            raise TypeError(f"Unknown relationship type: {self.type}")

    def _sql(self):
        if self.type in ["one_to_many", "many_to_one"]:
            return self._one_to_many_statement()

        elif self.type == "many_to_many":
            return self._many_to_many_statement()

    def _one_to_many_statement(self):
        return f"""ALTER TABLE {self.child}\nADD CONSTRAINT {self.parent_field_name} FOREIGN KEY (id) REFERENCES {self.parent} (id);\n\n"""

    def _many_to_many_statement(self):
        # see https://stackoverflow.com/questions/9789736/how-to-implement-a-many-to-many-relationship-in-postgresql
        t1 = self.parent
        t2 = self.child
        pk1 = f"{t1}_id"
        pk2 = f"{t2}_id"
        rel_table_name = f"{t1}_{t2}"
        return f"""CREATE TABLE IF NOT EXISTS {rel_table_name} (\n{TAB}{pk1} integer REFERENCES {t1} (id) ON UPDATE CASCADE ON DELETE CASCADE,\n{TAB}{pk2} integer REFERENCES {t2} (id) ON UPDATE CASCADE,\n{TAB}CONSTRAINT {rel_table_name}_pk PRIMARY KEY ({pk1}, {pk2})\n);\n\n"""
