import pdb

from .constants import TAB
from .concatenation_field import ConcatenationField
from .formula_field import FormulaField

class View:
    """ Generate Postgres table view sql of all table columns plus formula fields """

    def __repr__(self):
        return f"<View {self.name}>"

    def __init__(self, table):
        # where data is knack "objects" list from app data

        self.table = table

        self.name = f"{table.name_postgres}_view"

        self.formula_fields = [field for field in self.table.fields if isinstance(field, FormulaField) and field.sql]
        
        self.concat_fields = [field for field in self.table.fields if isinstance(field, ConcatenationField) and field.sql]
        
        self._set_dependencies()

        self._gather_tables()

        self.sql = self._to_sql()

    def _set_dependencies(self):
        self.depends_on = [field.rel_table_name for field in self.formula_fields]

    def _gather_tables(self):
        tables = [self.table.name_postgres]
        
        for field in self.concat_fields:
            tables += field.tables

        self.tables = list(set(tables))


    def _to_sql(self):
        sql = [f"SELECT {self.table.name_postgres}.*"]
        
        sql += [field.sql for field in self.formula_fields]

        sql += [field.sql for field in self.concat_fields]
        
        sql = f",\n{TAB}".join(sql)

        return f"""CREATE VIEW {self.name} AS\n{TAB}{sql}\n FROM {", ".join(self.tables)};\n\n"""
