"""
Here we define the components of each Knack Formula function, and reference a handler
function that will be used by the MethodHandler class to transform the function and
it's args into SQL syntax.

See: https://support.knack.com/hc/en-us/articles/115005002328-Text-Formula-Functions
"""
METHOD_DEFINITIONS = {
    "getDateDayOfWeekName": {
        "args": {"sql_name": "to_char"},
        "handler": "_get_day_of_week_name",
    },
    "getDateMonthOfYearName": {
        "args": {"sql_name": "to_char"},
        "handler": "_get_month_name",
    },
    "trim": {"args": {"sql_name": "TRIM"}, "handler": "_default_handler"},
    "trimLeft": {"args": {"sql_name": "LTRIM"}, "handler": "_default_handler"},
    "trimRight": {"args": {"sql_name": "RTRIM"}, "handler": "_default_handler"},
    "length": {"args": {"sql_name": "CHAR_LENGTH"}, "handler": "_default_handler"},
    "lower": {"args": {"sql_name": "LOWER"}, "handler": "_default_handler"},
    "upper": {"args": {"sql_name": "UPPER"}, "handler": "_default_handler"},
    "capitalize": {"args": {"sql_name": "INITCAP"}, "handler": "_default_handler"},
    "random": {
        "args": {  # not supported. could implement a custom method: https://www.simononsoftware.com/random-string-in-postgresql/
            "sql_name": None
        },
        "handler": "_default_handler",
    },
    "numberToWords": {
        "args": {  # not support. could implement a custom method: https://stackoverflow.com/questions/14486108/converting-any-number-in-words
            "sql_name": None
        },
        "handler": "_default_handler",
    },
    "left": {"args": {"sql_name": "LEFT"}, "handler": "_default_handler"},
    "right": {"args": {"sql_name": "RIGHT"}, "handler": "_default_handler"},
    "mid": {"args": {"sql_name": "SUBSTRING"}, "handler": "_default_handler"},
    "regexReplace": {"args": {"sql_name": None}, "handler": "_default_handler"},
    "extractRegex": {
        "args": {"sql_name": "REGEXP_REPLACE"},  # 3 params not supported by parser
        "handler": "_default_handler",
    },
    "replace": {"args": {"sql_name": "REPLACE"}, "handler": "_default_handler"},
}
