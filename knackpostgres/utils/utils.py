
def escape_single_quotes(string):
    return string.replace("'", "\\\'")
    
def wrap_single_quotes(string):
    return f"\'{string}\'"

def clean_whitespace(string):
    """
    Remove leading/trailing whitespace, and internal whitespace > 1 space.
    WARNING: will remove multiple whitespace characters from quoted strings. E.g.,
    `stringy_field  DEFAULT 'default   with multiple whitespace'` becomes 
    `stringy_field  DEFAULT 'default with multiple whitespace'`
    """
    return ' '.join(string.split())

def valid_pg_name(original_name):
    """
    Convert an arbitrary string into a postgres-compliant name.
    Try not too make it too ugly while you're at it.

    Warning, this opens up the possiblity that input `original_name` will
    no longer be unique to it's class instance.

    E.g., names `2-A` and `2$A` both resolve to `_2_A`.

    Best practice: label your knack fields and objects with postgres-compliant names.

    Docs: https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html
    """

    # todo: use the whole dang list: https://www.postgresql.org/docs/current/sql-keywords-appendix.html
    FORBIDDEN_POSTGRES_NAMES = [
        "user",
        "default",
        "unique",
        "number",
        "limit",
        "table",
        "column",
        "view",
    ]

    # first, make lowercase and replace spaces with underscores
    new_name = original_name.lower().replace(" ", "_")

    # first character cannot be a number. if so, put underscore in front of it
    new_name = new_name if not new_name[0].isdigit() else "_" + new_name

    # replace non-alphanum chars with underscore
    new_name = "".join(e if e.isalnum() else "_" for e in new_name)

    if new_name in FORBIDDEN_POSTGRES_NAMES:
        new_name = f"_{new_name}"

    return new_name
