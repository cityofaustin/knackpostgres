class DataHandler:
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

        if val == "":
            # todo: this could vary by handler
            return None

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
        return val

    def _file_handler(self, val):
        return val.get("url")

    def _image_handler(self, val):
        # image will be a url or key/val pair
        try:
            return val.get("url")

        except AttributeError:
            return val.strip()  # noticed some leading white space in data tracker

    def _date_time_handler(self, val):
        return val.get("iso_timestamp")

    def _email_handler(self, val):
        return val.get("email")
