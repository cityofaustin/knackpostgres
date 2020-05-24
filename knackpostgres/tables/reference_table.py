from .table import Table


class ReferenceTable(Table):
    """
    Create a `reference` or `associative` table in which to store many-to-many relationship references.
    """
    def __init__(self, data, schema):
        super().__init__(data, schema)
        
        self._drop_knack_id()
        

    def _drop_knack_id(self):
        for i, field in enumerate(self.fields):
            if field.name_postgres == "knack_id":
                del self.fields[i]