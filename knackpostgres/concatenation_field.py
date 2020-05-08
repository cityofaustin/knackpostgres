import re

from .field_def import FieldDef
from .parsers import get_parser
from .method_handler import MethodHandler


# todo: are you actually surfacing concatenated text in nested functions?
# i think not


# Regex search expressions
# match: field_xx or field_xx.field_xx (if it's enclosed in braces)
FIELD_SEARCH_EXCLUDE_BRACES = "(?:{)(field_\d+)(?:})|(?:{)(field_\d+.field_\d+)(?:})"

# match: {field_xx} or {field_xx.field_xx}
FIELD_SEARCH_INCLUDE_BRACES = "({field_\d+})|({field_\d+.field_\d+})"


class ConcatenationField(FieldDef):
    """
    Field wrapper/parser of Knack concatenation (aka `text formula`) fields.

    in the words of Gob Bluth, i didn't take `wasn't optimistic it could be done` for an answer
    """

    def __init__(self, data, table):
        super().__init__(data, table)

        self.equation = self.format_knack.get("equation")
        # todo: consider when the foreign table is the host
        # todo: i think you need to use alter views for all formula fields,
        # as they can be cross-dependent. but you can first test if your dependency logic is working
        # for connection formulae, you need to do (SELLECT ..... WHERE .... FROM)

    def handle_formula(self, app, grammar="concatenation"):
        self.app = app
        self._get_fieldmap()
        self.parser = get_parser(grammar)
        self.tree = self.parser.parse(self.equation)
        self._process_methods()
        self._gather_all_sql()
        self._to_sql()
        return self

    def _process_methods(self):
        """
        Traverse up through the Lark tree that is comprised of the contents of
        the knack formula field definition. when a method is reached, collect
        it's child components (arbitrary strings or other methods). In this way,
        we roll up each sub-component of the field definition from it's component 
        parts, translating each part to SQL syntax along the way.
        """
        for node in self.tree.iter_subtrees():
            if node.data == "_values":
                continue

            elif node.data == "method":
                self._handle_method(node)

    def _parse_arg(self, arg):
        """
        Parse an argument of a Knack formula field string method.
        Args can be a combination of arbitrary strings and other string methods
        """
        if arg.data == "second_arg" and len(arg.children) == 1:
            # try to convert second arg to an int, and if so return it as a stringified number
            try:
                return str(int(arg.children[0].children[0].value))
            except:
                pass

        arg_substrings = []

        for elem in arg.children:
            arg_type = elem.data

            if arg_type == "text_content":
                text_content = elem.children[0].value
                substrings = self._parse_fieldnames(text_content)

                arg_substrings += substrings

            elif arg_type == "method":
                arg_substrings.append(elem.sql)

        return f"CONCAT({', '.join(arg_substrings)})" if len(arg_substrings) > 1 else arg_substrings[0]

    def _handle_method(self, method):
        """
        Translate a knack string method to sql. 
        Because we're iterating 
        """
        method.args = []

        for elem in method.children:
            name = elem.data

            if name == "method_name":
                method.name = elem.children[0].value

            elif "arg" in name:
                method.args.append(self._parse_arg(elem))

        handler = MethodHandler(method)
        method.sql = handler.handle_method()
        return None

    def _gather_all_sql(self):
        """
        At this point, all of the method nodes in our tree have a `sql` attribute, and
        all that's left to do is to create the sql syntax for the top-level text
        elements (which can be comprised of aribitrary strings or Knack field names, e.g. {field_101})
        """
        self.tree.sql = []

        for elem in self.tree.children:
            if elem.data == "text_content":
                text_content = elem.children[0].value
                substrings = self._parse_fieldnames(text_content)
                self.tree.sql += substrings
            elif elem.data == "method":
                self.tree.sql.append(elem.sql)

    def _get_fieldmap(self):
        """
        Generate an index of knack fieldames and their postgres fieldnames.
        We have to look across all tables in the app to accomplish this, because
        formula fields may reference fields in other tables.

        Also, collect all the table names involved in this field, so we can include
        them in our SQL statement

        Also, `uses_connection` as true/false. We need to use a different SQL syntax
        for a formula that relies on connection fields.
        """
        self.uses_connection = False
        self.fieldmap = {}
        self.tables = []
        self.connection_fields = []

        fieldname_matches = re.findall(FIELD_SEARCH_EXCLUDE_BRACES, self.equation)
        
        # and we need to unpack the results, which are touples of capturing groups. a tubple will
        # either have a value in first position (for normal field) or second position (connection field)
        fieldnames = [field[0] for field in fieldname_matches if field[0]]
        fieldnames += [field[1] for field in fieldname_matches if field[1]]

        # reduce to unique
        fieldnames = list(set(fieldnames))

        for fieldname in fieldnames:
            try:
                # attempt to handle connected field
                conn_fieldname = fieldname.split(".")[0]
                target_fieldname = fieldname.split(".")[1]
                self.uses_connection = True
                
            except IndexError:
                target_fieldname = fieldname
                conn_fieldname = None
                pass

            target_field = self.app.find_field_from_field_key(target_fieldname)

            if conn_fieldname:
                conn_field = self.app.find_field_from_field_key(conn_fieldname)

                self.connection_fields.append(conn_field)
                
            if target_field.table.name not in self.tables:
                self.tables.append(target_field.table.name)

            self.fieldmap[fieldname] = f"{target_field.table.name}.{target_field.name_postgres}"

        return self

    def _parse_fieldnames(self, text_content):
        """
        Split a string into it's fieldname and non-fieldname parts. wrapping the non-fieldnames parts
        in single quotes, as SQL requires, and replacing fieldnames with their postgres fieldnames

        we include braces in our field search, because we must know which substrings are syntactical {field_xx}
        calls, or if for some reason your text formula has a non-field value like `field_99` :|        
        """

        #         try:
        #     fieldnames = [f"{{fieldname}}" for fieldname in self.fieldmap.keys()]

        # except TypeError:
        #     # this content is a non-string value, presuming integer
        #     return [text_content]

        field_search = re.compile(FIELD_SEARCH_INCLUDE_BRACES)

        # fetch the known fieldnames in this formula from the fieldmap, adding braces as mentioned above
        try:
            fieldnames = [f"{{{fieldname}}}" for fieldname in self.fieldmap.keys()]
        except AttributeError:
            print("no fieldnames")
            print(self.equation)
            pass

        # split the string into it's components of fieldnames and non-fieldnames
        substrings = field_search.split(text_content)

        bob = field_search.findall(text_content)
        
        # remove None values and empty strings, an artecfact of regex.findall
        substrings = [sub for sub in substrings if sub != "" and sub != None]

        # replace the fieldname elements with their postgres fieldname
        # wrap non-fieldnames in single quotes, for sql
        for i, sub in enumerate(substrings):

            if sub in fieldnames:
                substrings[i] = self.fieldmap[sub.replace("{", "").replace("}", "")]
            else:
                # don't forget to escape single quotes!
                sub = sub.replace("'", "\\'")
                substrings[i] = f"'{sub}'"

        # remove empty strings, which are an artefact of regex splitting
        return [sub for sub in substrings if sub != "''"]


    def _add_select_where_statements(self):

        views = [f"{table_name}_view" for table_name in self.tables]

        where_clauses = []

        for conn_field in self.connection_fields:
            
            dest_join_field = conn_field.name_postgres

            if self.table.name_postgres == conn_field.name_postgres:
                rel_table_name = conn_field.rel_table_name
            else:
                rel_table_name = conn_field.table.name_postgres
        
            rel_table_name = f"{rel_table_name}_view"

            where_clause = f"""WHERE {rel_table_name}.{dest_join_field} = {self.table.name}.id"""
            where_clauses.append(where_clause)

        all_where_clauses = " AND ".join(where_clauses)
        view_names = ", ".join(views)

        return f"""(SELECT {self.sql} FROM {view_names} {all_where_clauses}) AS {self.name_postgres}"""

    def _to_sql(self):
        """ 
        At this point, every node in our tree has a `sql` attribute, they merely need
        to be concatenated.
        """
        
        self.sql = f"""CONCAT({', '.join(self.tree.sql)})"""

        if (self.uses_connection):
            self.sql = self._add_select_where_statements()
        else:
            self.sql = f"""{self.sql} AS {self.name_postgres}"""

        return self.sql


        