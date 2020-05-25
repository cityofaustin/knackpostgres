from knackpostgres.utils.utils import clean_whitespace, escape_single_quotes, wrap_single_quotes, valid_pg_name

class Field:
    """ Base class for `Field` definition wrappers """

    def __repr__(self):
        return f"<{type(self).__name__} '{self.name_postgres}'>"

    def __init__(self, data, name, table):
        self.table = table
        self.data = data
        self.name_postgres = valid_pg_name(name)
        
        # default proprerties
        self.data_type = data.get("data_type", None)
        self.is_primary_key = data.get("is_primary_key", False)
        self.default = data.get("default", None)
        self.constraints = data.get("constraints", None)

        # to be populated by public method `to_sql`
        self.sql = None

    def _format_default(self):
        default = self.default

        if default == None or default == "":
            return ""
        
        elif type(default) == bool:
            default = str(default).upper()

        elif self.data_type == "NUMERIC":
            default = float(default)

        elif type(default) == str:
            default = escape_single_quotes(self.default)
            default = wrap_single_quotes(default)

        return f"DEFAULT {default}"

    def to_sql(self):

        pk = "PRIMARY KEY" if self.is_primary_key else ""

        default = self._format_default()

        constraints = " ".join(self.constraints) if self.constraints else ""

        sql = f"{self.name_postgres} {self.data_type} {pk} {default} {constraints}".strip()
        
        self.sql = clean_whitespace(sql)
        return self.sql
