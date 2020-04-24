from .field_def import FieldDef
from .constants import FIELD_DEFINITIONS


class StandardField(FieldDef):
    """ A Knack foruma field definition wrapper """
    
    def __init__(self, data, table):
        super().__init__(data, table)

    def to_sql(self):

        pk = "PRIMARY KEY" if self.primary_key else ""

        default = self._format_default()

        constraints = " ".join(self.constraints) if self.constraints else ""

        self.sql = f"{self.name_postgres} {self.data_type} {pk} {default}{constraints}".strip()
        return self.sql