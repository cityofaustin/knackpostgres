from ._knack_field import KnackField
from knackpostgres.config.constants import FIELD_DEFINITIONS


class ConnField(KnackField):
    """ A Knack foruma field definition wrapper """

    def __init__(self, data, name, table):
        super().__init__(data, name, table)

        self.relationship_type = self._relationship_type()

    def _relationship_type(self):

        has = self.relationship_knack["has"]
        belongs_to = self.relationship_knack["belongs_to"]

        return f"{has}_to_{belongs_to}"
