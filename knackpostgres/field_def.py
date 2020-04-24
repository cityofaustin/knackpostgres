from .constants import FIELD_DEFINITIONS, TAB
from .utils import valid_pg_name


class FieldDef:
    """ Base class for Knack `field` definition wrappers """

    def __repr__(self):
        return f"<{type(self).__name__} '{self.name_postgres}'>"

    def __init__(self, data, table, primary_key=False):
        """
        No knack field is used as a primary key. 
        We generate a primary key field while handling each table, during
        which we set primary_key = `True`
        """
        self.table = table

        self.primary_key = primary_key

        for key in data:
            setattr(self, key + "_knack", data[key])

        self.sql = None

        # convert field name to underscore and presever original
        self.name_postgres, self.name_knack = valid_pg_name(self.name_knack)

        self.default_postgres = self._set_default()

        self.constraints = self._get_constraints()

        self.data_type = self._postgres_data_type(self.type_knack)

        self._handle_rules()

    def _postgres_data_type(self, type_knack):

        try:
            data_type_postgres = FIELD_DEFINITIONS.get(type_knack).get("type_postgres")

        except AttributeError:
            raise AttributeError(f"Unsupported knack type: {type_knack}")

        return (
            f"{data_type_postgres}[]"
            if self._handle_array_type()
            else data_type_postgres
        )

    def _handle_array_type(self):
        """
        Identify array field types.

        Returns False if field is not multi-choice, else True

        PG Docs: https://www.postgresql.org/docs/9.1/arrays.html
        """
        try:
            # type options are ["single", "radios", "multi"]
            if self.format_knack["type"] == "multi":
                return True

        except (TypeError, AttributeError, KeyError):
            """
            some fields have no "format_knack" attr (AttributeError), others have
            a format_knack attr as `noneType` (TypeError), and still others have a format_knack
            attr, but no `type` key (KeyError)
            """
            return False

        # todo handle format elsewhere
        self.options = self.format_knack.get("options")

        self.blank = self.format_knack.get("blank")

        self.sorting = self.format_knack.get("sorting")

    def _set_default(self):

        if not hasattr(self, "default_knack"):
            return None

        if self.default_knack == "":
            return None

        else:
            return self.default_knack

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

        return constraints if constraints else None

    def _format_default(self):

        default = self.default_postgres

        # i must be missing something, but an inline if was not properly handling an empty string string
        if default == None:
            return ""

        elif type(default) == bool:
            default = str(default).upper()

        elif self.data_type == "NUMERIC":
            # knack provides numeric defaults as strings :/
            default = float(default)

        elif type(default) == str:
            # escape any single quotes
            default = default.replace("'", "\\'")
            default = f"'{default}'"

        return f"DEFAULT {default} "