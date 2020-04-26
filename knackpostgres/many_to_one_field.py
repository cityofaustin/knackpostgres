from .connection_field import ConnField
from .constants import FIELD_DEFINITIONS


class ManyToOneField(ConnField):
    """ A Knack foruma field definition wrapper """
    
    def __init__(self, data, table):
        super().__init__(data, table)

    def to_sql(self):

        pk = "PRIMARY KEY" if self.primary_key else ""

        default = self._format_default()

        # todo enable these after data is loaded
        # constraints = " ".join(self.constraints) if self.constraints else ""
        
        constraints = ""

        self.sql = f"{self.name_postgres} {self.data_type} {pk} {default}{constraints}".strip()
        return self.sql

    def handle_relationship(self, host_table_name, rel_table_name):
        """
        one to many and many to one relationships are handled here, simply
        by adding  column to the "host" table that reference the primary key
        field of the related table. we're not using foreign keys.
    
        if the "host" table is the parent table, we append the array `[]` 
        modifier so that it can hold multiple values. good luck
        sorting that out in Hasura.

        this is a public method that must be called by the app class
        and passed down to the parent table and then fields, because
        relationships reach across tables in the app.

        this must be done after all fields (except formulae) have been
        instanciated, because we reference postgres field and table
        outside of the host table.

        the `data_type` of these fields is always NUMERIC, and is 
        defined in .constanstants.FIELD_DEFINITIONS
        """
        self.rel_table_name = rel_table_name
        
        self.name_postgres = f"{self.name_postgres}_rel_{self.rel_table_name}_id"

        if self.relationship_type == "many_to_one":
            # set one-to-many field as array type
            self.data_type = f"{self.data_type}[]"

        return self