from .field_def import FieldDef
from .constants import FIELD_DEFINITIONS


class ConnField(FieldDef):
    """ A Knack foruma field definition wrapper """
    
    def __init__(self, data, table):
        super().__init__(data, table)

        self.relationship_type = self._relationship_type()

    def _relationship_type(self):

        has = self.relationship_knack["has"]
        belongs_to = self.relationship_knack["belongs_to"]

        return f"{has}_to_{belongs_to}"
