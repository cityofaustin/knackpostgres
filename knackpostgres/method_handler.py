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
        self.args = METHOD_DEFINITIONS[self.method.name]["args"]
        self.handler = getattr(self, METHOD_DEFINITIONS[self.method.name]["handler"])

    def _join_method_sql(self, elements):
        params = ", ".join(elements["params"])
        return f"{elements['name']}({params})"

    def handle_method(self):
        return self.handler(**self.args)

    def _get_month_name(self, dt):
        return f"to_char({dt}, 'Month')"

    def _get_day_of_week_name(self, dt):
        return f"to_char({dt}, 'Day')"

    def _default_handler(self, sql_name=None):
        args = ", ".join(self.method.params)
        sql = f"{sql_name}({args})"
        return sql
