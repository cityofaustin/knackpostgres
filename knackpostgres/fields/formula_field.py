from .field_def import FieldDef
from .equation import Equation
from knackpostgres.config.constants import FIELD_DEFINITIONS


class FormulaField(FieldDef):
    """ A Knack foruma field definition wrapper """
    
    def __init__(self, data, table):
        super().__init__(data, table)
    
    def handle_formula(self, app):

        if FIELD_DEFINITIONS[self.type_knack].get("is_standard_equation"):
            self.sql = self._handle_standard_equation(app)

        elif self.type_knack == "equation":
            # TODO
            self.sql = self._handle_custom_equation(app)

        else:
            return None

    def _handle_custom_equation(self, app):
        return Equation(self, self.table)

    def _handle_standard_equation(self, app):
        """
        Handle Knack formula field, setting formula attributes
        and the self.sql to the formula's `select` statement.

        This method must be run after the app has been instanciated,
        because formulae reference tables/fields across the entire
        app.

        Input:
        - self: `FieldDef` class
        - app: the parent `App` class

        Returns formula dict with keys:
        - method: the name of pg formula method
        - child_field: the field on which the formula will operate on
        - parent_table: the foreign table on which to which child records are related
        - 
        """
        pg_method = self.type_knack.upper()

        self.method = "AVG" if pg_method == "AVERAGE" else pg_method

        self.host_table_name = self.table.name_postgres

        self.name = self.name_postgres
        
        if self.method == "COUNT":
            # for counts, always just count the primary key
            self.dest_field_name = "id"

        else:
            dest_field_key = self.format_knack["field"]["key"]

            self.dest_field_name = app.find_field_from_field_key(
                dest_field_key, return_attr="name_postgres"
            )

        try:
            # count connections key is a string
            self.connection_field_key = self.format_knack["connection"].get("key")
        
        except AttributeError:
            # other connections are a dict with "key"
            self.connection_field_key = self.format_knack["connection"]

        self.connection_field = app.find_field_from_field_key(self.connection_field_key)

        self.dest_join_field = self.connection_field.name_postgres

        if self.connection_field.relationship_type == "many_to_many":

            return self._many_to_many_formula(app)
        else:
            return self._one_to_many_formula(app)
        
    def _many_to_many_formula(self, app):
        self.rel_table_name = self.connection_field.rel_table_name
        self.rel_table_view_name = f"{self.rel_table_name}_view"
        self.reference_table_name = self.connection_field.reference_table_name

        return f"""(SELECT {self.method}({self.rel_table_view_name}.{self.dest_field_name}) as {self.name_postgres}
            FROM {self.rel_table_view_name}
            JOIN {self.reference_table_name} 
            ON {self.reference_table_name}.{self.rel_table_name}_id = {self.rel_table_view_name}.id
            AND {self.reference_table_name}.{self.host_table_name}_id = {self.host_table_name}.id)
        """

    def _one_to_many_formula(self, app):

        if self.table.name_postgres == self.connection_field.table.name_postgres:
            self.rel_table_name = self.connection_field.rel_table_name
        else:
            self.rel_table_name = self.connection_field.table.name_postgres
        
        self.rel_table_name = f"{self.rel_table_name}_view"

        return f"""(SELECT {self.method}({self.rel_table_name}.{self.dest_field_name}) FROM {self.rel_table_name} WHERE {self.rel_table_name}.{self.dest_join_field} = {self.host_table_name}.id) AS {self.name_postgres}"""

    def to_sql(self):
        return self.sql