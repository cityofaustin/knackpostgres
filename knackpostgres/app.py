"""
Convert a Knack application to a PostgreSQL Database.
"""
import logging
from pathlib import Path
from pprint import pprint as print
import shutil

from knackpy import get_app_data

from knackpostgres.fields.formula_field import FormulaField
from knackpostgres.fields.concatenation_field import ConcatenationField
from knackpostgres.tables.knack_table import KnackTable
from knackpostgres.tables.metadata_table import MetaTable
from knackpostgres.tables.reference_table import ReferenceTable
from knackpostgres.tables.view import View
from knackpostgres.scene import Scene
from knackpostgres.utils.utils import valid_pg_name


APP_ATTRIBUTES = [
    # todo: implement explicit setting
    {"name": "app_id", "source": "built_in"},
    {"name": "id", "source": "knack"},
    {"name": "metadata", "source": "built_in"},
    {"name": "metadata_schema", "source": "built_in"},
    {"name": "metadata_knack", "source": "built_in"},
    {"name": "name", "source": "knack"},
    {"name": "obj_filter", "source": "built_in"},
    {"name": "obj_lookup", "source": "built_in"},
    {"name": "objects", "source": "knack"},
    {"name": "scenes", "source": "knack"},
    {"name": "schema", "source": "built_in"},
    {"name": "tables", "source": "built_in"},    
    {"name": "views", "source": "built_in"},
]

class App:
    """
    Knack application wrapper. Stores app meta data, tables, fields, etc.
    Receives a Knack application ID str and returns App instance.
    
    Usage:
    >>> `app = App("myappid")`
    >>> app.to_sql()   # write to /sql dir
    """
    def __repr__(self):
        return f"<App {self.name}> ({len(self.objects)} objects)"

    def __init__(
        self, app_id, obj_filter=None, schema="public", metadata_schema="_meta",
    ):

        self.app_id = app_id

        # optionally include only object keys specified in filter
        self.obj_filter = obj_filter

        # all data will be written to `schema`, except for metadata, which writes to 
        # `metadata_schema`
        self.schema = valid_pg_name(schema)
        self.metadata_schema = valid_pg_name(metadata_schema)

        self.metadata_knack = self._get_app_data()

        # assign knack metadata to class attributes
        for key in self.metadata_knack:
            setattr(self, key, self.metadata_knack[key])

        self.tables = self._generate_tables()

        self.obj_lookup = self._generate_obj_lookup()

        self._update_one_to_many_relationships()

        self.tables += self._update_many_to_many_relationships()

        self._handle_formulae()

        # These are database views, not Knack "views" ;)
        self.views = (
            self._handle_views()
        )

        self.scenes = self._handle_scenes()

        self.metadata = self._set_metadata()

        self.schema_sql = self._generate_schema_sql()

        logging.info(self)

    def to_sql(self, path="sql", overwrite=False):
        """
        Write application SQL commands to file. Alternatively, use the `Loader` class
        to connect/write directly from the `App` class.
        """
        if overwrite:
            shutil.rmtree(path)

        self._write_sql(self.schema_sql, path, "schema", self.schema)

        for table in self.tables:
            self._write_sql(table.to_sql(), path, "tables", table.name_postgres)

        for view in self.views:
            self._write_sql(view.sql, path, "views", view.name)

    def _generate_schema_sql(self):
        schema = [self.schema, self.metadata_schema]

        schema_sql = [
            f"CREATE SCHEMA IF NOT EXISTS {schema_name};" for schema_name in schema
        ]
        return "\n".join(schema_sql)

    def _write_sql(self, sql, path, subdir, name_attr, method="w"):

        file_path = Path(path) / subdir

        file_path.mkdir(exist_ok=True, parents=True)

        file_path = file_path / f"{name_attr}.sql"

        with open(file_path, method) as fout:
            fout.write(sql)

    def _get_app_data(self):
        return get_app_data(self.app_id)

    def _generate_tables(self):
        if self.obj_filter:
            return [
                KnackTable(obj, obj["name"], self.schema)
                for obj in self.objects
                if obj["key"] in self.obj_filter
            ]
        else:
            return [KnackTable(obj, obj["name"], self.schema) for obj in self.objects]

    def _handle_views(self):
        return [View(table) for table in self.tables]

    def _generate_obj_lookup(self):
        """ The obj_lookup allows us to find connected object keys across the entire app """
        return {table.key_knack: table.name_postgres for table in self.tables}

    def _update_one_to_many_relationships(self):
        # sets field definitions for relationship fields,
        # which require references to other tables
        for table in self.tables:
            table.update_one_to_many_relationships(self.obj_lookup)
            # update field map referecnces in table (used by translator)
            table.create_field_map()

    def _update_many_to_many_relationships(self):
        """
        Ah, many-to-many relationships. To handle these, we need
        to create an associative table which holds relationships across two
        tables. We accomplish this by parsing each relationship definition
        and calling a new `Table` class with the appriate `Field` classes.

        Obviously this all needs to happen after all other tables and fields
        have been instanciated (except for formulae, which rely on relationships)
        so that we can reference the postgres database table and field names.
        """
        fields = self._gather_many_to_many_relationships()

        tables = []

        for field in fields:
            field.set_relationship_references(self)
            reference_table_name = field.reference_table_name
            tables.append(
                KnackTable(
                    field.reference_table_data,
                    reference_table_name,
                    self.schema,
                    associative=True,
                )
            )

        return tables

    def _gather_many_to_many_relationships(self):

        fields = []

        for table in self.tables:
            for field in table.fields:
                try:
                    if field.relationship_type == "many_to_many":
                        fields.append(field)

                except AttributeError:
                    continue
        return fields

    def _handle_formulae(self):
        for table in self.tables:
            for field in table.fields:
                if isinstance(field, FormulaField) or isinstance(
                    field, ConcatenationField
                ):
                    field.handle_formula(self)

        return self.tables

    def find_table_from_object_key(self, key, return_attr=None):
        for table in self.tables:
            if table.key_knack == key:
                return table if not return_attr else getattr(table, return_attr)
        return None

    def find_field_from_field_key(self, key, return_attr=None):
        """
        from a knack field key, track down the `Field` instance
        """
        for table in self.tables:
            for field in table.fields:
                try:
                    if field.key_knack == key:
                        try:
                            return (
                                field
                                if not return_attr
                                else getattr(field, return_attr)
                            )
                        except AttributeError:
                            # we found the field, but it's missing the requested attribute
                            return None
                except AttributeError:
                    # primary key fields do not have `knack` field propeties and are ignored
                    continue

        else:
            return None

    def find_table_from_field_key(self, key, return_attr=None):
        """
        From a knack field key, track down the table in which that field lives """
        try:
            # some times the connection is under "key", and somtimes it's a string literal
            key = key.get("key")

        except AttributeError:
            pass

        for table in self.tables:
            for field in table.fields:
                if field.key_knack == key:
                    # we found it :)
                    return table if not return_attr else getattr(table, return_attr)

        # no table found that contains this key
        return None

    def _handle_scenes(self):
        scenes = []

        for scene in self.scenes:
            scenes.append(Scene(scene))

        return scenes


    def _set_metadata(self):
        """
        TODO: these metadata table names are currently hardcoded as
        `_fields`, `_pages`, and `_sections`. Move to config.
        """
        metadata = []
        fields = [field for table in self.tables for field in table.fields]
        metatable_fields = MetaTable(fields, "_fields", self.metadata_schema)
        metatable_fields.to_sql()
        metadata.append(metatable_fields)
        views = [view for scene in self.scenes for view in scene._views]
        metatable_views = MetaTable(views, "_views", self.metadata_schema)
        return metadata
