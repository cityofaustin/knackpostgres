from knackpostgres.utils.utils import valid_pg_name


class Field:
    """ Base class for `Field` definition wrappers """

    def __repr__(self):
        return f"<{type(self).__name__} '{self.name_postgres}'>"

    def __init__(self, data, name, table):
        """
        Note that no knack fields are used as primary keys. The Knack built-in `id`
        field is converted to `id_knack` and used by the `Loader` class when populating
        connection fields. We generate a primary key (type = `_pg_primary_key`) field
        in the base `Table` class on __init__.
        """
        self.table = table
        self.data = data
        self.name_postgres = valid_pg_name(name)
        self.is_primary_key = True if self.name_postgres == "id" else False
        self.sql = None
