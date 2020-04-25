import pdb

from .constants import TAB
from .formula_field import FormulaField

class View:
    """ Generate Postgres table view sql of all table columns plus formula fields """

    def __repr__(self):
        return f"<View {self.name}>"

    def __init__(self, table):
        # where data is knack "objects" list from app data

        self.table = table

        self.name = f"{table.name_postgres}_view"

        self.formulaFields = [field for field in self.table.fields if isinstance(field, FormulaField) and field.sql]
        
        self._set_dependencies()

        self.sql = self.to_sql()

    def _set_dependencies(self):
        self.depends_on = [field.rel_table_name for field in self.formulaFields]

    def to_sql(self):
        sql = [f"SELECT {self.table.name_postgres}.*"]
        
        sql += [field.sql for field in self.formulaFields]
        
        sql = f",\n{TAB}".join(sql)

        return f"""CREATE VIEW {self.name} AS\n{TAB}{sql}\n FROM {self.table.name_postgres};\n\n"""
