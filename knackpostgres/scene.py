from pprint import pprint as print

from .view_knack import ViewKnack
from knackpostgres.config.metadata import METADATA_FIELDS
from knackpostgres.utils.utils import valid_pg_name
    
import pdb

# NOT IMPLEMENTED SCENE ATTRS
# unsupported_scene_attrs = [
#     'authentication_profiles_knack',
#     'ignore_entry_scene_menu_knack',
#     'print_knack',
#     'modal_knack',
#     'page_menu_display_knack',
# ]


class Scene:
    """ Base class for Knack `scene` definition wrappers """

    def __repr__(self):
        return f"<{type(self).__name__} {self.name}>"

    def __init__(self, data):
        scene_attrs = [field["name"] for field in METADATA_FIELDS["_pages"]]

        for key in data:
            if key in scene_attrs:
                setattr(self, key, data[key])

        self._handle_views()

    def _handle_views(self):
        self._views = []
        for view in self.views:
            self._views.append(ViewKnack(view))
