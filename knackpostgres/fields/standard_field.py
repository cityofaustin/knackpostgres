from ._knack_field import KnackField
from knackpostgres.config.constants import FIELD_DEFINITIONS


class StandardField(KnackField):
    """ Knack foruma field definition wrapper """

    def __init__(self, data, name, table):
        super().__init__(data, name, table)
        pass
