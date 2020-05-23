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
    Translate Knack records to destination postgresql schema. Generate insert and update
    statements data loading.
    """

    def __repr__(self):
        return f"<Translator {self.knack.obj} to {self.table.name_postgres}>"

    def __init__(self, knack, table):
        # where knack is a knackpy.Knack object and table is a Table class instance
        self.knack = knack
        self.table = table

        if not self.knack.data_raw:
            logging.warning(f"{self}: no records to process.")
            raise IndexError(f"No records found at {self.knack.obj}")

        self.knack.data_raw = self._replace_raw_fieldnames()
        self.records = self._translate_records()
        self.records = self._convert_fieldnames()
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

        for record in self.records:
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

        for record in self.records:
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

        for record in self.records:
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

        for record in self.records:
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

    def to_sql(self, path="data"):
        path = Path.cwd() / path
        path.mkdir(exist_ok=True)
        self.fname = path / (self.table.name_postgres + ".sql")

        statements = []

        with open(self.fname, "w") as fout:

            for record in self.records:
                columns = record.keys()
                columns = ", ".join(columns)

                values = ", ".join([f"'{val}'" for key, val in record.items()])

                sql = f"""INSERT INTO {self.table.name_postgres} ({columns}) VALUES\n({values});\n\n"""

                sql = sql.replace(f"'{PG_NULL}'", "NULL")

                fout.write(sql)
                statements.append(sql)

        logging.info(f"{self.fname} - {len(self.records)} records")
        self.sql = statements

    def to_csv(self, records=None, path="data"):

        if not records:
            records = self.records

        path = Path.cwd() / path
        path.mkdir(exist_ok=True)
        self.fname = path / (self.table.name_postgres + ".csv")

        with open(self.fname, "w") as fout:

            self.fieldnames = [field for field in records.keys()]

            writer = csv.DictWriter(
                fout, fieldnames=self.fieldnames, quotechar="'", delimiter="|"
            )

            # we don't write the header. because postgresql
            # doesn't want it. you can fetch them from self.fieldnames
            # writer.writeheader()

            for record in records:
                writer.writerow(record)

        print(f"{self.fname} - {len(self.records)} records")
