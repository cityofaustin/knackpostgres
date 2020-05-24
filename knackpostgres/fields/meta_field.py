from knackpostgres.fields._field import Field


class MetaField(Field):
    """ Wrapper for metadata field defintions """

    def __init__(self, data, name, table):
        super().__init__(data, name, table)
        self.default_postgres = data.get("default")
        self.constraints = data.get("constraints")

        # Note that unlike Knack fields, we assume the input `type` to be a valid
        # postgres data_type
        self.data_type = data["type"]

    def _format_default(self):

        default = self.default_postgres

        if default == None or default == "":
            return ""

        elif type(default) == bool:
            default = str(default).upper()

        elif self.data_type == "NUMERIC":
            # knack provides numeric defaults as strings :/
            default = float(default)

        elif type(default) == str:
            # escape any single quotes
            default = default.replace("'", "\\'")
            default = f"'{default}'"

        return f"DEFAULT {default} "

    def to_sql(self):

        pk = "PRIMARY KEY" if self.is_primary_key else ""

        default = self._format_default()

        constraints = " ".join(self.constraints) if self.constraints else ""

        self.sql = (
            f"{self.name_postgres} {self.data_type} {pk} {default}{constraints}".strip()
        )
        return self.sql
