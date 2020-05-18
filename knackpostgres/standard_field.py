from .field_def import FieldDef
from .constants import FIELD_DEFINITIONS


class StandardField(FieldDef):
    """ Knack foruma field definition wrapper """
    
    def __init__(self, data, table):
        super().__init__(data, table)
        pass