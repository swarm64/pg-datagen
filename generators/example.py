
from collections import OrderedDict

from lib.base_object import BaseObject


class Example(BaseObject):
    TABLE_NAME = 'example'

    @classmethod
    def _schema(cls, rand_gen):
        return (
            lambda: OrderedDict([
                ('id', rand_gen.uuid),
                ('value', rand_gen.random_text(1, 100))
            ])
        )

    @classmethod
    def sample(cls, rand_gen, num_rows):
        data = super(Example, cls).sample(cls._schema(rand_gen), num_rows)
        return data
