from pprint import pprint as print

from knackpostgres.config.constants import TAB
from knackpostgres.fields.concatenation_field import ConcatenationField
from knackpostgres.fields.formula_field import FormulaField


class View:
    """ Generate Postgres table view sql of all table columns plus formula fields """

    def __repr__(self):
        return f"<View {self.name}>"

    def __init__(self, table):
        # where data is knack "objects" list from app data

        self.table = table

        self.name = f"{table.name_postgres}_view"

        self.formula_fields = [
            field
            for field in self.table.fields
            if isinstance(field, FormulaField) and field.sql
        ]

        self.concat_fields = [
            field
            for field in self.table.fields
            if isinstance(field, ConcatenationField) and field.sql
        ]

        self._set_dependencies()

        self._create_join_clauses()

        self.sql = self._to_sql()

    def _set_dependencies(self):
        self.depends_on = [field.rel_table_name for field in self.formula_fields]

    def _create_join_clauses(self):
        """
        Create join statements for any concat field that uses a connection field

        # todo: handle many-to-many concats
        # todo: currently assuming that concat's table is the connection host :/
        """

        joins = []

        for field in self.concat_fields:
            for conn_field in field.connection_fields:
                rel_table_name = conn_field.rel_table_name
                host_field_name = conn_field.name_postgres
                joins.append(
                    f"""LEFT OUTER JOIN {rel_table_name} ON ({self.table.name_postgres}.{host_field_name} = {rel_table_name}.id)"""
                )

        self.joins = "\n ".join(joins)

    def _to_sql(self):
        sql = [f"SELECT {self.table.name_postgres}.*"]

        sql += [field.sql for field in self.formula_fields]

        sql += [field.sql for field in self.concat_fields]

        sql = f",\n{TAB}".join(sql)

        return f"""CREATE VIEW {self.name} AS\n{TAB}{sql}\n FROM {self.table.name_postgres} {self.joins};\n\n"""
