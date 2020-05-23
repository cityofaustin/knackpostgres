PG_NULL = "$$NULL$$"

TAB = "    "

FIELD_DEFINITIONS = {
    "short_text": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "paragraph_text": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "phone": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "link": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "multiple_choice": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "rich_text": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "file": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "image": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "email": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "signature": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "password": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "concatenation": {
        "type_postgres": "TEXT",
        "is_formula": True,
        "is_standard_equation": False,
    },
    "count": {"type_postgres": "NUMERIC", "is_formula": True, "is_standard_equation": True},
    "max": {"type_postgres": "NUMERIC", "is_formula": True, "is_standard_equation": True},
    "min": {"type_postgres": "NUMERIC", "is_formula": True, "is_standard_equation": True},
    "sum": {"type_postgres": "NUMERIC", "is_formula": True, "is_standard_equation": True},
    "equation": {
        "type_postgres": None,
        "is_formula": True,
        "is_standard_equation": False,
    },
    "average": {
        "type_postgres": "NUMERIC",
        "is_formula": True,
        "is_standard_equation": True,
    },
    "date_time": {
        "type_postgres": "TIMESTAMP WITH TIME ZONE",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "timer": {
        "type_postgres": "TIMESTAMP WITH TIME ZONE",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "number": {
        "type_postgres": "NUMERIC",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "currency": {
        "type_postgres": "NUMERIC",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "auto_increment": {
        "type_postgres": "NUMERIC",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "rating": {
        "type_postgres": "NUMERIC",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "address": {
        "type_postgres": "JSON",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "name": {
        "type_postgres": "JSON",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "user_roles": {
        "type_postgres": "TEXT[]",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "boolean": {
        "type_postgres": "BOOLEAN",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "connection": {
        "type_postgres": "NUMERIC",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "_knack_id": {
        "type_postgres": "TEXT",
        "is_formula": False,
        "is_standard_equation": False,
    },
    "_pg_primary_key": {
        "type_postgres": "SERIAL",
        "is_formula": False,
        "is_standard_equation": False,
    },
}
