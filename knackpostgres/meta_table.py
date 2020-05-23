class MetaTable:
    """ Create a table in which to store app field metadata """

    def __repr__(self):
        return f"<Table {self.name_postgres}> ({len(self.fields)} fields)"

    def __init__(self, app):
        """ where app is an `App` class instance """

        self.name_postgres = "_meta_fields"

        self.fields = self._get_fields()

    def _get_fields(self):
        return [
            {"name": "id", "constraints": " PRIMARY KEY", "type": "SERIAL"},
            {"name": "is_primary_key", "constraints": "", "type": "BOOLEAN"},
            {"name": "column_name", "constraints": "", "type": "TEXT"},
            {"name": "type", "constraints": "", "type": "TEXT"},
            {"name": "table_name", "constraints": "", "type": "TEXT"},
            {"name": "view_name", "constraints": "", "type": "TEXT"},
            {"name": "options", "constraints": "", "type": "TEXT[]"},
            {"name": "read_only", "constraints": "", "type": "BOOLEAN"},
            {"name": "is_formula", "constraints": "", "type": "BOOLEAN"},
            {"name": "relationships", "constraints": "", "type": "JSON"},
        ]

    def _field_to_sql(self, name, constraints, data_type):
        return f"{name} {data_type}{constraints}"

    def to_sql(self):
        fields_sql = [
            self._field_to_sql(field["name"], field["constraints"], field["type"])
            for field in self.fields
        ]
        all_fields_sql = f",\n    ".join(fields_sql)

        self.sql = f"""CREATE TABLE IF NOT EXISTS {self.name_postgres} (\n    {all_fields_sql}\n);\n\n"""
        return self.sql
