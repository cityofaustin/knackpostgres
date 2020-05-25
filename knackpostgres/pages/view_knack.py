from pprint import pprint as print
from knackpostgres.config.metadata import METADATA_FIELDS
from knackpostgres.utils.utils import valid_pg_name

import pdb


# NOT IMPLEMENTED ATTRIBUTES
# ignored_view_attributes = [
#     "links",
#     'action',
#     'display_pagination_below',
#     'allow_preset_filters',
#     'registration_type',
#     'hide_empty',
#     'filter_fields',
#     'totals',
#     'no_data_text',
#     'rows_per_page',
#     'allow_limit',
#     'menu_filters',
#     'preset_filters',
#     "filter_type",
# ]

class ViewKnack:
    """ Base class for Knack `scene` definition wrappers """

    def __repr__(self):
        return f"<{type(self).__name__} {self.name} {self.type}>"

    def __init__(self, data):
        view_attrs = [field["name"] for field in METADATA_FIELDS["_views"]]

        for key in data:
            if key in view_attrs:
                setattr(self, key, data[key])