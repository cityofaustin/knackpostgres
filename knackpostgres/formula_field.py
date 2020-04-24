from .field_def import FieldDef
from .constants import FIELD_DEFINITIONS


class FormulaField(FieldDef):
    """ A Knack foruma field definition wrapper """
    
    def __init__(self, data, table):
        super().__init__(data, table)

    
    def handle_formula(self, app):

        if FIELD_DEFINITIONS[self.type_knack].get("is_standard_equation"):
            self.sql = self._handle_standard_equation(app)

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

        self.host_table_name = self.table.name

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
        self.reference_table_name = self.connection_field.reference_table_name

        return f"""(SELECT {self.method}({self.rel_table_name}.{self.dest_field_name}) as {self.name_postgres}
            FROM {self.rel_table_name}
            JOIN {self.reference_table_name} 
            ON {self.reference_table_name}.{self.rel_table_name}_id = {self.rel_table_name}.id
            JOIN {self.host_table_name}
            ON {self.reference_table_name}.{self.host_table_name}_id = {self.host_table_name}.id)
        """

    def _one_to_many_formula(self, app):
        
        if self.table.name_postgres == self.connection_field.table.name_postgres:
            self.rel_table_name = self.connection_field.rel_table_name
        else:
            self.rel_table_name = self.connection_field.table.name_postgres

        return f"""(SELECT {self.method}({self.rel_table_name}.{self.dest_field_name}) FROM {self.rel_table_name} WHERE {self.rel_table_name}.{self.dest_join_field} = {self.host_table_name}.id) AS {self.name_postgres}"""

    def to_sql(self):
        return self.sql
        
# name: count_of_orders
# host: customers
# 'connection': 'field_43',
# self.connection_field.relationship_type = one to many
# self.connection_field.rel_table_name = customers << prbolem. use table name

# on toppings count pizzas.id over pizza connection

# bob = """
# SELECT count(pizzas.{def field} (id) })
#   FROM orders
#   JOIN many_pizzas_to_many_orders 
#     ON many_pizzas_to_many_orders.orders_id = orders.id
#   JOIN pizzas
#     ON many_pizzas_to_many_orders.pizzas_id = pizzas.id
#     WHERE pizzas.pizza_name = 'sausage';
# """

# bob = """
# SELECT COUNT(pizzas.id) as count_of_pizzas
#             FROM pizzas
#             JOIN many_toppings_to_many_pizzas 
#             ON many_toppings_to_many_pizzas.pizzas_id' = pizzas.id
#             JOIN toppings
#             ON many_toppings_to_many_pizzas.toppings_id' = toppings.id
# """
