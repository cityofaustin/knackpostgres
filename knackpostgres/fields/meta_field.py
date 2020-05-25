from knackpostgres.fields._field import Field
from knackpostgres.exceptions.exceptions import ValidationError

class MetaField(Field):
    """ Wrapper for metadata field defintions """

    def __init__(self, data, name, table):
        super().__init__(data, name, table)
        
        self.accessor = data.get("accessor", None)

        # except KeyError:
        #     print(data)
        #     raise ValidationError("Data does not contain an `accessor` property.")

