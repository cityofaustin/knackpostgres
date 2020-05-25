import csv
import json
import logging
from pathlib import Path
import re

import requests

from knackpostgres.fields.many_to_one_field import ManyToOneField
from knackpostgres.fields.many_to_many_field import ManyToManyField
from knackpostgres.utils.data_handlers import DataHandlers
from knackpostgres.utils.utils import escape_single_quotes, wrap_single_quotes
from knackpostgres.config.constants import PG_NULL


# TODO: you're better than this
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

TEMPLATE = """
mutation insert_fields {
  insert_$table(objects: [$objects])
   { returning {
        id
      }
  }
}
"""

# TODO: figure out graphql json nulls
class Translator:
    """
    Base class for Translators.
    """
    def __init__(self, table, data):
        self.data = data
        self.table = table
        self.field_type_map = self._generate_field_type_map()
    
    def post(self):   
        endpoint = "http://localhost:8080/v1/graphql"

        objects = ", ".join(self.graphql)
    
        objects = self._replace_nulls(objects)
        
        mutation = TEMPLATE.replace("$objects", objects)

        if self.table.schema == "public":
            mutation = mutation.replace("$table", f"{self.table.name_postgres}")
        else:
            mutation = mutation.replace("$table", f"{self.table.schema}_{self.table.name_postgres}") 

        res = requests.post(endpoint, json={"query": mutation})

        if "errors" in res.text:
            print(mutation)
            print(res.text)

        return None


    def _replace_nulls(self, string):
        return string.replace(f"\"{PG_NULL}\"", "null")

    def _replace_quoted_keys(self, string): 
        KEY_SEARCH = '\"\w+\":'

        key_finder = re.compile(KEY_SEARCH)
        keys = key_finder.findall(string)

        for k in keys:
            dequoted = f"{k[1:-2]}:"
            string = string.replace(k, dequoted)

        return string

    def _row_to_graphql(self, row):
        """
        30 min of searching did not turn up any packages for this. so, John's attempt
        """
        # convert lists to postgres arrays

        delete_keys = []

        for key, value in row.items():
            data_type = self.field_type_map[key]

            if data_type == "JSON" and value == PG_NULL:
                delete_keys.append(key)

            elif data_type.endswith("[]"):
                if value == PG_NULL:
                    continue

                values = []

                for val in value:
                    if type(val) == str:
                        val = f'\'{val}\''
                    elif type(val) == None:
                        val = 'null'

                    values.append(val)

                pg_array =  ", ".join([val for val in values])

                row[key] = f"{{{pg_array}}}"

        for key in delete_keys:
            row.pop(key)

        # json gets us most of the way there
        graphql = json.dumps(row)

        # just need to replace quoted keys
        graphql = self._replace_quoted_keys(graphql)

        return graphql

    def to_graphql(self):

        graphql = []

        for row in self.data:
             graphql.append(self._row_to_graphql(row))

        self.graphql = graphql
        self.post()
        return None

    def _generate_field_type_map(self):
        return { field.name_postgres: field.data_type for field in self.table.fields}


class KnackTranslator(Translator):
    """
    Translate Knack records to destination postgresql schema. Generate insert and update
    statements data loading.
    """

    def __repr__(self):
        return f"<KnackTranslator {self.knack.obj} to {self.table.name_postgres}>"

    def __init__(self, table, data, knack):
        super().__init__(table, data)

        # where `knack` is a knackpy.Knack object and `table` is a Table class instance
        self.knack = knack

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
