import pdb
import re

from .field_def import FieldDef

SEARCH_EXPRS = {
    "fields": "{(.+?)}",  # between curly braces {abc} 
    "inner": "}(.+?){", # between closed brace and open brace  }abc{
    "prefix": "(^.*?){", # before the first open brace abc{
    "postfix": "(?!.*}).*" # after the last open brace }abc
}

class ConcatenationField(FieldDef):
    """
    Field wrapper/parser of Knack concatenation (aka `text formula`) fields
    """

    def __init__(self, data, table):
        super().__init__(data, table)

        self.equation = self.format_knack.get("equation")

    def handle_formula(self, app):
        eq = self.equation
        # some concats have linebreak cruft
        eq = eq.replace("\n", "")
        
        if "." in eq:
            # complex concat, with connection calls
            # TODO
            return None

        prefix = self._findall(eq, SEARCH_EXPRS["prefix"])
        inner = self._findall(eq, SEARCH_EXPRS["inner"])
        postfix = self._findall(eq, SEARCH_EXPRS["postfix"])
        fields = self._findall(eq, SEARCH_EXPRS["fields"])
        fields_postgres = []

        for field in fields:
            name_postgres = app.find_field_from_field_key(field, return_attr="name_postgres")
            fields_postgres.append(name_postgres)

        self.elements = self._arrange_elements(fields_postgres, prefix, inner, postfix)
        self._to_sql()
        return self

    def _findall(self, eq, expr):
        return re.findall(expr, eq)

    def _arrange_elements(self, fields, prefix, inner, postfix):
        elements = [f"'{prefix[0]}'"] if prefix[0] != "" else []

        for i, field in enumerate(fields):
            elements.append(f"{field}")
            try:
                elements.append(f"'{inner[i]}'")
            except IndexError:
                continue

        elements = elements + [f"'{postfix}'"] if postfix[0] != "" else elements

        return elements

    def _to_sql(self):
        concats = ", ".join(self.elements)
        self.sql = f"""CONCAT({concats}) AS {self.name_postgres}"""
        return self.sql

