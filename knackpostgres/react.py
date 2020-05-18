import json
from pathlib import Path

class React:
    """ Create React App Configuration Files from `knackpostgres` App """
    def __repr__(self):
        return f"<{type(self).__name__}> {self.app.name}"

    def __init__(self, app):
        self.app = app
        self.field_defs = self._create_field_defs()

    def _create_field_defs(self):
        field_defs = []

        for table in self.app.tables:
            for field in table.fields:
                field_defs.append(self._create_field_def(field))

        return field_defs

    def _create_field_def(self, field):
        return {
            "_column": field.name_postgres,
            "_table": field.table.name_postgres,
            "_component": "TextField",
            "_primary_key": field.primary_key,
            "_type": field.data_type,
            "_field_props": {
                "required": True if True else False,
                "InputProps": { "readOnly": False }, # todo: handle in react app logic
                "label": field.name_knack,
            }
        }

    def to_json(self):
        self._write_json("field_defs", self.field_defs)

    def _write_json(self, name_attr, data, path="react", method="w"):

        file_path = Path(path)

        file_path.mkdir(exist_ok=True, parents=True)

        file_path = file_path / f"{name_attr}.json"

        with open(file_path, method) as fout:
            fout.write(json.dumps(data))
