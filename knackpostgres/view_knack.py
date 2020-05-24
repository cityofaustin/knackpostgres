from pprint import pprint as print
from knackpostgres.utils.utils import valid_pg_name


class ViewKnack:
    """ Base class for Knack `scene` definition wrappers """

    def __repr__(self):
        return f"<{type(self).__name__} {self.name_knack} {self.type_knack}>"

    def __init__(self, data):
        """
        No knack field is used as a primary key. 
        We generate a primary key field while handling each table, during
        which we set primary_key = `True`
        """
        for key in data:
            setattr(self, key + "_knack", data[key])
