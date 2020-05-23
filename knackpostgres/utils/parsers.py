"""
Lark parser definitions for handling knack foruma fields.
"""
from lark import Lark

CONCATENATION = r"""
    _values: (method | (text_before_method|text_without_method) )+

    method.1: (_method_with_two_param | _method_with_one_param)
    
    _method_with_two_param.1: method_name_two_param _OPEN_PARENS first_arg _COMMA second_arg _CLOSED_PARENS

    _method_with_one_param.2: method_name_one_param _OPEN_PARENS only_arg _CLOSED_PARENS

    first_arg: (_values | text_before_comma)+
    
    second_arg: text_before_closed_parens

    only_arg: _values | text_before_closed_parens

    _CLOSED_PARENS: /\)/
    
    _OPEN_PARENS: /\(/

    method_name_one_param: /(trim|trimLeft|trimRight|length|lower|upper|capitalize|random|numberToWords|getDateMonthOfYearName|getDateDayOfWeekName)/ -> method_name

    method_name_two_param: /(left|right|mid|regexReplace|extractRegex|replace)/ -> method_name

    text_before_method: /.+?(?=((trim|trimLeft|trimRight|length|lower|upper|capitalize|random|numberToWords|getDateMonthOfYearName|getDateDayOfWeekName|left|right|mid|regexReplace|extractRegex|replace)\())/ -> text_content
    
    text_without_method: /.+/  -> text_content
    
    text_before_comma: /.+?(?=,)/ -> text_content
  
    text_before_closed_parens: /.+?(?=\))/ -> text_content
    
    _COMMA: /,/
"""

GRAMMARS = {"concatenation": {"grammar": CONCATENATION, "entry_point": "_values"}}


def get_parser(grammar_name):
    grammar = GRAMMARS[grammar_name]["grammar"]
    # # i find `start` to be confusing, hence renaming it to `entry_point` in config
    entry_point = GRAMMARS[grammar_name]["entry_point"]
    return Lark(grammar, start=entry_point, debug=False, propagate_positions=True)
