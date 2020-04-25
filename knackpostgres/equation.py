import pdb

import re

class Equation:
    """ A Knack equation wrapper """
    
    def __init__(self, field, table):
        self.field = field
        self.table = table
        self.equation = self.field.format_knack.get("equation")
        self.equation_type = self.field.format_knack["equation_type"]
        self.equation_parsed = self._parse_equation()

    def _parse_equation(self):
        # https://stackoverflow.com/questions/38999344/extract-string-within-parentheses-python
        
        # eq = "((innner) (another))"
        # sub_eqs = re.search('\(([^)]+)', eq)
        
        if sub_eqs:
            pdb.set_trace()

        eq = self.equation

        # many equations have linebreak cruft
        eq = eq.replace("\n", "")
        pdb.set_trace()
        # e.g. {field_43.field_56} + {field_43.field_51}

