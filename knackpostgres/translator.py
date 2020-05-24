import csv
import logging
from pathlib import Path

from knackpostgres.fields.many_to_one_field import ManyToOneField
from knackpostgres.fields.many_to_many_field import ManyToManyField
from knackpostgres.data_handlers import DataHandlers
from knackpostgres.config.constants import PG_NULL


IGNORE_FIELD_TYPES = [
    "concatenation",
    "max",
    "min",
    "count",
    "sum",
    "average",
    "signature",
    "equation",
]


class Translator:
    """
    Base class for Translators.
    """

    def __init__(self):
        self.data = []
        self.table = None
        pass

    def _values_sql(self, values):

        values_sql = []

        for value in values:
            if type(value) == list:
                value_str = ", ".join([f'"{val}"' for val in value])
                values_sql.append(f"'{{{value_str}}}'")
            else:
                values_sql.append(f"'{value}'")

        return ", ".join([sql for sql in values_sql])

    def to_sql(self, path="data"):
        path = Path.cwd() / path
        path.mkdir(exist_ok=True)
        self.fname = path / (self.table.name_postgres + ".sql")

        statements = []

        with open(self.fname, "w") as fout:

            for row in self.data:
                columns = ", ".join(row.keys())
                values = self._values_sql(row.values())
                sql = f"""INSERT INTO {self.table.schema}.{self.table.name_postgres} ({columns}) VALUES\n({values});\n\n"""
                sql = sql.replace(f"'{PG_NULL}'", "NULL")
                fout.write(sql)
                statements.append(sql)

        logging.info(f"{self.fname} - {len(self.data)} rows")
        self.sql = statements


class KnackTranslator(Translator):
    """
    Translate Knack records to destination postgresql schema. Generate insert and update
    statements data loading.
    """

    def __repr__(self):
        return f"<KnackTranslator {self.knack.obj} to {self.table.name_postgres}>"

    def __init__(self, knack, table):
        super().__init__()

        # where `knack` is a knackpy.Knack object and `table` is a Table class instance
        self.knack = knack
        self.table = table

        if not self.knack.data_raw:
            raise IndexError(f"No records found at {self.knack.obj}")

        self.knack.data_raw = self._replace_raw_fieldnames()
        self.data = self._translate_records()
        self.data = self._convert_fieldnames()
        self.connection_data = self._extract_one_to_many_connections()
        self.connection_data += self._extract_many_to_many_connections()
        self._drop_connection_fields()

    def connections_sql(self):
        if not self.connection_data:
            return []

        for record in self.connection_data:
            if record.get("reference_table_name"):
                record["sql"] = self._insert_statement_many_to_many(**record)
            else:
                record["sql"] = self._update_statement_many_to_one(**record)

        return [record["sql"] for record in self.connection_data]

    def _update_statement_many_to_one(self, **kwargs):
        return f"""UPDATE {kwargs["host_table_name"]} SET ({kwargs["field_name"]}) =
            (SELECT id FROM {kwargs["rel_table_name"]}
            WHERE {kwargs["rel_table_name"]}.knack_id = '{kwargs["conn_record_id"]}')
            WHERE knack_id = '{kwargs["knack_id"]}';"""

    def _extract_one_to_many_connections(self):

        conn_fields = {
            field.name_postgres: field
            for field in self.table.fields
            if isinstance(field, ManyToOneField)
        }

        if not conn_fields:
            return []

        conn_data = []

        for record in self.data:
            for field in record.keys():
                if field in conn_fields:
                    vals = record[field]
                    if not vals:
                        continue

                    rel_table_name = conn_fields[field].rel_table_name

                    if isinstance(vals, list):
                        for val in vals:
                            conn_data.append(
                                self._connection_record(
                                    field, record["knack_id"], val["id"], rel_table_name
                                )
                            )
                    else:
                        conn_data.append(
                            self._connection_record(
                                field, record["knack_id"], vals["id"], rel_table_name
                            )
                        )

        return conn_data

    def _connection_record(
        self,
        field_name,
        knack_id,
        conn_record_id,
        rel_table_name,
        reference_table_name=None,
    ):
        return {
            "host_table_name": self.table.name_postgres,
            "field_name": field_name,
            "knack_id": knack_id,
            "conn_record_id": conn_record_id,
            "rel_table_name": rel_table_name,
            "reference_table_name": reference_table_name,
        }

    def _drop_connection_fields(self):
        # we handle conenciton fields with udpate statements
        # so drop them to keep them out of view of loader.insert_each
        conn_fieldnames = {
            field.name_postgres
            for field in self.table.fields
            if isinstance(field, ManyToOneField) or isinstance(field, ManyToManyField)
        }

        for record in self.data:
            for fname in conn_fieldnames:
                try:
                    record.pop(fname)
                except KeyError:
                    pass
        return None

    def _extract_many_to_many_connections(self):
        # todo: handle these fields
        conn_fields = {
            field.name_postgres: field
            for field in self.table.fields
            if isinstance(field, ManyToManyField)
        }

        if not conn_fields:
            return []

        conn_data = []

        for record in self.data:
            for field in record.keys():
                if field in conn_fields:
                    vals = record[field]

                    if not vals:
                        continue

                    rel_table_name = conn_fields[field].rel_table_name
                    reference_table_name = conn_fields[field].reference_table_name
                    if isinstance(vals, list):
                        for val in vals:
                            conn_data.append(
                                self._connection_record(
                                    field,
                                    record["knack_id"],
                                    val["id"],
                                    rel_table_name,
                                    reference_table_name=reference_table_name,
                                )
                            )
                    else:
                        conn_data.append(
                            self._connection_record(
                                field,
                                record["knack_id"],
                                vals["id"],
                                rel_table_name,
                                reference_table_name=reference_table_name,
                            )
                        )
        return conn_data

    def _insert_statement_many_to_many(self, **kwargs):

        return f"""
            INSERT INTO {kwargs["reference_table_name"]} ({kwargs["host_table_name"]}_id, {kwargs["rel_table_name"]}_id) VALUES (
                (SELECT id FROM {kwargs["host_table_name"]} WHERE knack_id = '{kwargs["knack_id"]}'),
                (SELECT id FROM {kwargs["rel_table_name"]} WHERE knack_id = '{kwargs["conn_record_id"]}')
            );
        """

    def _translate_records(self):
        translated_records = []

        for record in self.knack.data_raw:
            translated_record = {}

            for field in record.keys():

                try:
                    field_type = self.knack.fields.get(field).get("type")

                except AttributeError:
                    # knack internal fields are not exposed in field metadata
                    # which is fine, we skip them
                    continue

                if field_type in IGNORE_FIELD_TYPES:
                    continue

                # use the DataHandler to translate the data based on field type
                try:
                    handler = DataHandlers(field_type)
                except ValueError:
                    # a handler has been explicitly NOT defined for this field type
                    # field will be dropped
                    continue

                try:
                    translated_record[field] = handler.handle(record[field])

                except ValueError:
                    print(f"skipping empty string: {field_type}")
                    continue

            translated_records.append(translated_record)

        return translated_records

    def _replace_raw_fieldnames(self):
        """
        For any Knack field that has both a "raw" and formatted field, use the raw field and drop the
        formated (non-raw) field
        """

        # sample the first record to get all fieldnames
        # todo: this assumes all records have all fields, which is not gauraunteed
        # inspect knack data to hardcode which fields have raw fields
        record = self.knack.data_raw[0]

        raw_fields = [
            field.split("_raw")[0] for field in record.keys() if field.endswith("_raw")
        ]

        new_records = []

        for record in self.knack.data_raw:

            new_record = {}

            for field in record.keys():

                # lookup the destination postgres fieldname and replace keys accordingly
                if field == "id":
                    new_field = field

                elif field.endswith("_raw"):
                    new_field = field.split("_raw")[0]

                elif field not in raw_fields:
                    # this field does not have a raw field, so use this one
                    new_field = field

                else:
                    # ignore the field, assuming it's a dupe of the "raw" field
                    continue

                new_record[new_field] = record[field]

            new_records.append(new_record)

        return new_records

    def _convert_fieldnames(self):
        """
        Lookup the destination postgres fieldname and replace keys accordingly
        """
        new_records = []

        for record in self.data:
            new_record = {}

            for field in record.keys():

                if field == "profile_keys_raw":
                    # ignore this knack meta field
                    # todo: move elsewher
                    continue

                if field == "id":
                    # our App class expects knack ids to be represented with a "knack_id" fieldname
                    converted_field_name = "knack_id"

                else:
                    try:
                        converted_field_name = self.table.field_map.get(field).get(
                            "name"
                        )
                    except AttributeError:
                        # field was not defined. probably a passwordfield
                        print(
                            f"Warning: {self.knack.fields[field]['label']} ({field}) is not defined and will be ignored."
                        )
                        continue

                new_record[converted_field_name] = record[field]

            new_records.append(new_record)

        return new_records


class MetaTranslator(Translator):
    """
    Translate App metadata to destination postgresql schema.
    Skips all of the record translation from Knack, and instead just
    writes from records supplied in the App'a meta_table.
    """

    def __repr__(self):
        return f"<MetaTable {self.name_postgres}>"

    def __init__(self, metatable):
        super().__init__()
        self.table = metatable
        self.data = self.table.rows
