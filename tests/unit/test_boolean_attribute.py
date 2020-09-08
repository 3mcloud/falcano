'''
Tests for the Boolean Attribute
'''
from falcano.attributes import BooleanAttribute
from falcano.constants import BOOLEAN

# pylint: disable=no-self-use
class TestBooleanAttribute:
    '''
    Tests boolean attributes
    '''
    def test_boolean_attribute(self):
        '''
        BooleanAttribute.default
        '''
        attr = BooleanAttribute()
        assert attr is not None

        assert attr.attr_type == BOOLEAN # pylint: disable=no-member
        attr = BooleanAttribute(default=True)
        assert attr.default is True

    def test_boolean_serialize(self):
        '''
        BooleanAttribute.serialize
        '''
        attr = BooleanAttribute()
        assert attr.serialize(True) is True
        assert attr.serialize(False) is False
        assert attr.serialize(None) is None

    def test_boolean_deserialize(self):
        '''
        BooleanAttribute.deserialize
        '''
        attr = BooleanAttribute()
        assert attr.deserialize(True) is True
        assert attr.deserialize(False) is False
