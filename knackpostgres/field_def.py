import pdb

from .constants import FIELD_DEFINITIONS
from .utils import valid_pg_name


class FieldDef:
    """ A Knack `field` definition wrapper """

    def __repr__(self):
        return f"<FieldDef {self.name_postgres}> ({self.type_knack})"

    def __init__(self, data):
        self.primary_key = False
        self.constraints = None

        for key in data:
            setattr(self, key + "_knack", data[key])

        # convert field name to underscore and presever original
        self.name_postgres, self.name_knack = valid_pg_name(self.name_knack)

        self.default_postgres = self._set_default()

        self.constraints = self._get_constraints()

        self.data_type = self._postgres_data_type(self.type_knack)

        self._handle_rules()

        self.sql = self._to_sql() if self.data_type else None

    def _postgres_data_type(self, type_knack):

        try:
            return FIELD_DEFINITIONS.get(type_knack).get("type_postgres")

        except AttributeError:
            raise AttributeError(f"Unsupported knack type: {type_knack}")

    def _set_default(self):
        # all knack field objects have a "DEFAULT" key, possibly empty
        return "Hello"
        # # TODO: inspect the type of default each type has
        # if self.default_knack != None:
        #     print(self.type_knack)
        #     print(self.default_knack)
        #     print(type(self.default_knack))

        # return None

        # if True:
        #     try:
        #         # handle string-quoted number defaults
        #         return int(self.default_knack)
        #     except ValueError:
        #         return self.default_knack

    def _handle_rules(self):
        # SOMEDAY!
        # if hasattr(self, "rules"):
        #     if self.rules:
        #         print(self.rules)
        return None

    def _get_constraints(self):
        constraints = []

        if self.required_knack:
            constraints.append("NOT NULL")

        if self.unique_knack:
            constraints.append("UNIQUE")

        return constraints

    def _format_default(self):

        # i must be missing something, but an inline if was not properly handling an empty string string
        if self.default_postgres:
            default = self.default_postgres
        else:
            return " "

        try:
            # just in case a default val contains a single quote
            default = default.replace("'", "\\'")
        except AttributeError:
            pass

        default = str(default).upper() if type(default) == bool else f"'{default}'"

        return f"DEFAULT {default} "

    def _to_sql(self):

        pk = "PRIMARY KEY" if self.primary_key else ""

        default = self._format_default()

        constraints = " ".join(self.constraints) if self.constraints else ""

        return (
            f"{pk} {self.name_postgres} {self.data_type} {default}{constraints}".strip()
        )

    def _handle_date(self):
        date_format = self.format_knack["date_format"].lower()
        time_format = self.format_knack["time_format"].lower()

        if "ignore" in date_format:
            self.data_type = "TIME WITH TIME ZONE"
        else:
            self.data_type = "TIMESTAMP WITH TIME ZONE"

    def _handle_multi_choice(self):

        self.data_type = "text"  # postgres type

        try:
            # none type options are ["single", "radios", "multi"]
            if self.format_knack["type"] == "multi":
                self.array = True

        except KeyError:
            # if no "type", this is a knack user role field
            # so just continue for now
            # todo: look at this
            pass

        # todo handle format elsewhere
        self.options = self.format_knack.get("options")

        self.blank = self.format_knack.get("blank")

        self.sorting = self.format_knack.get("sorting")
