from .table import Table


class ReferenceTable(Table):
    def __init__(self, data):
        super().__init__(data)
        
        self._drop_knack_id()
        

    def _drop_knack_id(self):
        for i, field in enumerate(self.fields):
            if field.name_postgres == "knack_id":
                del self.fields[i]