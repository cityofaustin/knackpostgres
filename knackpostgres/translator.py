import csv
from pathlib import Path

from .handler_data import DataHandler

""" under construction """


class Translator:
    def __repr__(self):
        return f"<Translator {self.knack.obj}> to {self.table.name}"

    def __init__(self, knack, table):
        # where knack is a knackpy.Knack object and table is a Table class instance
        self.knack = knack
        self.table = table
        self.knack.data_raw = self._replace_raw_fieldnames()
        self.records = self._translate_records()
        self.records = self._convert_fieldnames()

    def _drop_forbidden_fields(self):

        return [
            field
            for field in knack.data_raw
            if field["name"].lower() not in FORBIDDEN_FIELD_NAMES
        ]

    def _drop_excluded_fields(self):

        new_records = []

        for record in self.knack.data_raw:
            new_record = {}

            for key in record.keys():
                try:
                    field_type = self.knack.fields[key].get("type")
                except KeyError:
                    # also dropping "account_status" and any other internal fields. TODO: warning
                    continue

                if field_type in EXCLUDED_TYPES:
                    print(field_type)
                    continue
                else:
                    new_record[key] = record[key]

            new_records.append(new_record)

        return new_records

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

                # use the DataHandler to translate the data based on field type
                try:
                    handler = DataHandler(field_type)
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
        For any Knack field that has both a "raw" field, use the raw field and drop the
        non-raw field
        """

        # sample the first record to get all fieldnames
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

    def to_csv(self, path="data"):

        path = Path.cwd() / path
        path.mkdir(exist_ok=True)
        fname = path / (self.table.name + ".csv")

        with open(fname, "w") as fout:

            fieldnames = [field for field in self.records[0].keys()]

            writer = csv.DictWriter(fout, fieldnames=fieldnames)

            writer.writeheader()

            for record in self.records:
                writer.writerow(record)

        print(f"{fname} - {len(self.records)} records")
