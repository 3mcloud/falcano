import json
from falcano.model import Model
from falcano.attributes import MapAttribute, NumberAttribute, UnicodeAttribute

class TestModel(Model):
    test_map = MapAttribute(attr_name="test_name", default={})

class TestCustomAttrMap(MapAttribute):
    overridden_number_attr = NumberAttribute(attr_name="number_attr")
    overridden_unicode_attr = UnicodeAttribute(attr_name="unicode_attr")

class TestDefaultsMap(MapAttribute):
    map_field = MapAttribute(default={})
    string_field = UnicodeAttribute(null=True)

def test_map_overridden_attrs_accessors():
    attr = TestCustomAttrMap(**{
        'overridden_number_attr': 10,
        'overridden_unicode_attr': "Hello"
    })
    assert attr.overridden_number_attr == 10
    assert attr.overridden_unicode_attr == "Hello"

def test_map_overridden_attrs_serialize():
    attribute = {
        'overridden_number_attr': 10,
        'overridden_unicode_attr': "Hello"
    }
    expected = {'number_attr': 10, 'unicode_attr': 'Hello'}
    assert TestCustomAttrMap().serialize(attribute) == expected

def test_additional_attrs_deserialize():
    raw_data = {
        'number_attr': {
            'N': '10'},
        'unicode_attr': {
            'S': 'Hello'
        },
        'undeclared_attr': {
            'S': 'Goodbye'
        }
    }
    expected = {
        'overridden_number_attr': 10,
        'overridden_unicode_attr': "Hello"
    }
    assert TestCustomAttrMap().deserialize(raw_data).attribute_values == expected

def test_null_attribute_subclassed_map():
    null_attribute = {
        'map_field': None
    }
    attr = TestDefaultsMap()
    serialized = attr.serialize(null_attribute)
    assert serialized == {}

def test_null_attribute_map_after_serialization():
    null_attribute = {
        'string_field': '',
    }
    attr = TestDefaultsMap()
    serialized = attr.serialize(null_attribute)
    assert serialized == {}

def test_defaults():
    item = TestDefaultsMap()
    assert item.validate()
    assert TestDefaultsMap().serialize(item) == {
        'map_field': {
        }
    }

def test_raw_set_attr():
    item = TestModel()
    item.test_map = {}
    item.test_map.foo = 'bar'
    item.test_map.num = 3
    item.test_map.nested = {'nestedfoo': 'nestedbar'}

    assert item.test_map['foo'] == 'bar'
    assert item.test_map['num'] == 3
    assert item.test_map['nested']['nestedfoo'] == 'nestedbar'

def test_raw_set_item():
    item = TestModel()
    item.test_map = {}
    item.test_map['foo'] = 'bar'
    item.test_map['num'] = 3
    item.test_map['nested'] = {'nestedfoo': 'nestedbar'}

    assert item.test_map['foo'] == 'bar'
    assert item.test_map['num'] == 3
    assert item.test_map['nested']['nestedfoo'] == 'nestedbar'

def test_raw_map_from_dict():
    item = TestModel(
        test_map={
            "foo": "bar",
            "num": 3,
            "nested": {
                "nestedfoo": "nestedbar"
            }
        }
    )

    assert item.test_map['foo'] == 'bar'
    assert item.test_map['num'] == 3

def test_raw_map_json_serialize():
    raw = {
        "foo": "bar",
        "num": 3,
        "nested": {
            "nestedfoo": "nestedbar"
        }
    }

    serialized_raw = json.dumps(raw, sort_keys=True)
    serialized_attr_from_raw = json.dumps(
        TestModel(test_map=raw).test_map.as_dict(),
        sort_keys=True)
    serialized_attr_from_map = json.dumps(
        TestModel(test_map=MapAttribute(**raw)).test_map.as_dict(),
        sort_keys=True)

    assert serialized_attr_from_raw == serialized_raw
    assert serialized_attr_from_map == serialized_raw

def test_typed_and_raw_map_json_serialize():
    class TypedMap(MapAttribute):
        test_map = MapAttribute()

    class SomeModel(Model):
        key = NumberAttribute(hash_key=True)
        typed_map = TypedMap()

    item = SomeModel(
        typed_map=TypedMap(test_map={'foo': 'bar'})
    )

    assert json.dumps({'test_map': {'foo': 'bar'}}) == json.dumps(item.typed_map.as_dict())

def test_attribute_paths_wrapping():
    class InnerMapAttribute(MapAttribute):
        test_map = MapAttribute(attr_name='dyn_test_map')

    class MiddleMapAttributeA(MapAttribute):
        inner_map = InnerMapAttribute(attr_name='dyn_in_map_a')

    class MiddleMapAttributeB(MapAttribute):
        inner_map = InnerMapAttribute(attr_name='dyn_in_map_b')

    class OuterMapAttribute(MapAttribute):
        mid_map_a = MiddleMapAttributeA()
        mid_map_b = MiddleMapAttributeB()

    class MyModel(Model):
        key = NumberAttribute(hash_key=True)
        outer_map = OuterMapAttribute(attr_name='dyn_out_map')

    mid_map_a_test_map = MyModel.outer_map.mid_map_a.inner_map.test_map
    mid_map_b_test_map = MyModel.outer_map.mid_map_b.inner_map.test_map

    assert mid_map_a_test_map.attr_name == 'dyn_test_map'
    assert mid_map_a_test_map.attr_path == ['dyn_out_map', 'mid_map_a', 'dyn_in_map_a', 'dyn_test_map']
    assert mid_map_b_test_map.attr_name == 'dyn_test_map'
    assert mid_map_b_test_map.attr_path == ['dyn_out_map', 'mid_map_b', 'dyn_in_map_b', 'dyn_test_map']

def test_nested_maps():

    class OuterMapAttribute(MapAttribute):
        mid_map = MapAttribute()

    class MyModel(Model):
        key = UnicodeAttribute(hash_key=True)
        outer_map = OuterMapAttribute()

    create_dict = {
        'key': 'id#1',
        'outer_map':{
            'mid_map':{
                'foo': 'bar',
            },
        },
    }
    test_model = MyModel(**create_dict)
    test_model.to_dict()
