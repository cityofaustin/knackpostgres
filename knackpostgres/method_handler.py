from .concatenation_methods import METHOD_DEFINITIONS


class MethodHandler:
    """
    Recieves a Knack string method node from a Lark parser and transform it to SQL syntax.

    This pattern allows us to to map Knack functions to SQL functions, which often but not
    always have the same name/structure.
    """
    def __repr__(self):
        return f"<MethodHandler `{self.method.name}`>"

    def __init__(self, method):

        self.method = method
        self.handler_args = METHOD_DEFINITIONS[self.method.name]["args"]
        self.handler = getattr(self, METHOD_DEFINITIONS[self.method.name]["handler"])

    def _join_method_sql(self, elements):
        args = ", ".join(elements["args"])
        return f"{elements['name']}({args})"

    def handle_method(self):
        return self.handler(**self.handler_args)

    def _get_month_name(self, sql_name=None):
        return f"{sql_name}({self.method.args[0]}, 'Month')"

    def _get_day_of_week_name(self, sql_name=None):
        return f"{sql_name}({self.method.args[0]}, 'Day')"

    def _default_handler(self, sql_name=None):
        args = ", ".join(self.method.args)
        sql = f"{sql_name}({args})"
        return sql
