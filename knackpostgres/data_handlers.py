from .constants import PG_NULL
import json

class DataHandlers:
    """ Translate Knack record values to destination DB values """

    def __repr__(self):
        return f"<Handler type=`{self.type}` name=`{self.handler.__name__}`>"

    def __init__(self, field_type):

        self.type = field_type

        try:
            self.handler = getattr(self, "_" + self.type + "_handler")
        except AttributeError:
            self.handler = getattr(self, "_default_handler")

    def handle(self, val):

        return self.handler(val)

    def _link_handler(self, val):

        return val.get("url")

    def _default_handler(self, val):
        """
        Handles these fieldtypes:
            auto_increment
            paragraph_text
            phone
            multiple_choice
            currency
            short_text
            name
            number
            boolean
        """
        if val == "":
            return PG_NULL
            
        return val
    
    def _user_roles_handler(self, val):
        if val == "":
            return "{}"
        import pdb; pdb.set_trace()
        return val

    def _phone_handler(self, val):
        return val.get("full")

    def _currency_handler(self, val):
        return float(val)

    def _name_handler(self, val):
        return json.dumps(val) if val else "{}"

    def _address_handler(self, val):
        return json.dumps(val) if val else "{}"

    def _file_handler(self, val):
        return val.get("url")

    def _image_handler(self, val):
        # image will be a url or key/val pair
        if val == []:
            return ""

        try:
            return val.get("url")

        except AttributeError:
            return val.strip()  # noticed some leading white space in data tracker

    def _date_time_handler(self, val):
        if val == "":
            return PG_NULL
        
        return val.get("iso_timestamp")

    
    def _timer_handler(self, val):
        return val["times"][0]["from"]["iso_timestamp"]

    def _email_handler(self, val):
        return val.get("email")
