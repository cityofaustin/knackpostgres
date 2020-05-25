from knackpostgres.config.constants import FIELD_DEFINITIONS, TAB
from knackpostgres.fields._field import Field
from knackpostgres.utils.utils import valid_pg_name


class KnackField(Field):
    """  Class for Knack `field` definition wrappers """

    def __init__(self, data, name, table):
        super().__init__(data, name, table)
        """
        Note that no knack field is used as a primary key. The Knack built-in `id`
        field is converted to `id_knack` and used by the loader when populating
        connection fields. We generate a primary key(type = `_pg_primary_key`) field
        in the base `Table` class on __init__.
        """
        for key in data:
            setattr(self, key + "_knack", data[key])

        self.default = self._set_default()
        self.constraints = self._get_constraints()
        self.data_type = self._postgres_data_type(self.type_knack)

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

    def _get_constraints(self):
        constraints = []

        if self.required_knack:
            constraints.append("NOT NULL")

        if self.unique_knack:
            constraints.append("UNIQUE")

        return constraints if constraints else None