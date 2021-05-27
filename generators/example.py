
from collections import OrderedDict

from lib.base_object import BaseObject

class Example(BaseObject):
    TABLE_NAME = 'example'

    @classmethod
    def schema(cls, rand_gen):
        return (
            lambda: OrderedDict([
                ('id', rand_gen.uuid()),
                ('value', rand_gen.string(100))
            ])
        )
