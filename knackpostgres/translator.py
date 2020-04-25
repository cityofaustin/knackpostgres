import csv
import logging
from pathlib import Path

from .data_handlers import DataHandlers
from .constants import PG_NULL

""" under construction """

IGNORE_FIELD_TYPES = ["connection", "concatenation", "max", "min", "count", "sum", "average", "signature", "equation"]

class Translator:
    def __repr__(self):
        return f"<Translator {self.knack.obj}> to {self.table.name_postgres}"

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

    
    
    def to_csv(self, path="data"):

        path = Path.cwd() / path
        path.mkdir(exist_ok=True)
        self.fname = path / (self.table.name_postgres + ".csv")

        with open(self.fname, "w") as fout:

            self.fieldnames = [field for field in self.records[0].keys()]

            writer = csv.DictWriter(fout, fieldnames=self.fieldnames, quotechar="'", delimiter='|')

            # we don't write the header. because postgresql 
            # doesn't want it. you can fetch them from self.fieldnames
            # writer.writeheader()

            for record in self.records:
                writer.writerow(record)

        print(f"{self.fname} - {len(self.records)} records")
