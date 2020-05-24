from knackpostgres.tables._table import Table


class ReferenceTable(Table):
    """
    Create a `reference` or `associative` table in which to store many-to-many relationship references.
    """

    def __init__(self, data, name, schema):
        super().__init__(data, name, schema)
        """
        TODO: there's a major refactor to do around `reference_table_data`
        it's really hard to untangle where how this data is used in the translator
        global search `reference_table_name` and you shall see
        """
        self._drop_knack_id()

    def _drop_knack_id(self):
        for i, field in enumerate(self.fields):
            if field.name_postgres == "knack_id":
                del self.fields[i]
