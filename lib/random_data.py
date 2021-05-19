
import json
from uuid import UUID

class RandomData:
    def __init__(self, rnd, uuid, data_type, serialization_type, binary_length):
        self.data = {
            'id': uuid,
            'type': str(data_type),
            'data': {
                'response': rnd.words(rnd.whole_number(100, 1000, 1)),
            },
            'binary': '' # rnd.unicode(binary_length)
        }
        self.serialization_type = serialization_type

    def __str__(self):
        if self.serialization_type == 'json':
            return self.to_json()

        if self.serialization_type == 'xml':
            return self.to_xml()

        raise ValueError(f'Unknown serialization type: { self.serialization_type }')

    def to_json(self):
        def uuid_convert(value):
            if isinstance(value, UUID):
                return value.hex

            return value

        return json.dumps(self.data, default=uuid_convert)


    def to_xml(self):
        return f'''<?xml version="1.0" encoding="UTF-8" ?>
<root>
    <id>{ self.data['id'] }</id>
    <type>{ self.data['type'] }</type>
    <data>
        <response>{ self.data['data']['response'] }</response>
    </data>
    <binary>
        <![CDATA[
            { self.data['binary'] }
        ]]>
    </binary>
</root>'''
