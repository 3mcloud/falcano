import unittest
from decimal import Decimal
from _pytest.monkeypatch import MonkeyPatch
from falcano.model import Model
from falcano.attributes import (
    UnicodeAttribute,
    ListAttribute,
    MapAttribute,
)

class TestModel(Model):
    '''Test model with meta'''
    class Meta(Model.Meta):
        table_name = 'falcano-map-attr-e2e'
        billing_mode = 'PAY_PER_REQUEST'
    PK = UnicodeAttribute(hash_key=True)
    SK = UnicodeAttribute(range_key=True)
    Data = MapAttribute()
    Type = UnicodeAttribute(default='test_map_attribute')

class TestMapAttribute(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fakeMyKeyId')
        self.monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fakeMySecret')
        if not TestModel.exists():
            TestModel.create_table(wait=True)

        for item in TestModel.scan():
            # clean up all items in db
            item.delete()

    def tearDown(self):
        # clean up all items in db
        for item in TestModel.scan():
            item.delete()

    def test_existence(self):
        test_dict = {
            'foo': 'bar',
            'baz': {
                'quux': 10,
            }
        }
        model = TestModel('test_map_attribute#A', 'test_map_attribute#B', Data=test_dict)
        model.save()

        res = TestModel.query('test_map_attribute#A', TestModel.SK.eq('test_map_attribute#B'))
        collected = res.collection()
        assert collected == {
            'test_map_attribute': {
                'Type': 'test_map_attribute',
                'SK': 'test_map_attribute#B',
                'Data': {'baz': {'quux': 10}, 'foo': 'bar'},
                'PK': 'test_map_attribute#A',
                'ID': 'A'
            }
        }
        assert isinstance(collected['test_map_attribute']['Data']['baz']['quux'], int)

        res.reset()
        for item in res:
            converted = item.to_dict(convert_decimal=False)

        assert isinstance(converted['Data']['baz']['quux'], Decimal)

    def test_nested_maps(self):
        class OuterMapAttribute(MapAttribute):
            mid_map = MapAttribute()

        class MyModel(Model):
            class Meta(Model.Meta):
                table_name = 'falcano-map-attr-e2e'
                billing_mode = 'PAY_PER_REQUEST'
            PK = UnicodeAttribute(hash_key=True)
            SK = UnicodeAttribute(range_key=True)
            outer_map = OuterMapAttribute()
            outer_list = ListAttribute()
            Type = UnicodeAttribute(default='test_nested_map')

        create_dict = {
            'PK': 'a',
            'SK': 'b',
            'outer_map': {
                'mid_map': {
                    'foo': 'bar',
                },
            },
            'outer_list': [
                OuterMapAttribute(**{'mid_map': {'baz': 'qux'}}),
            ]
        }
        test_model = MyModel(**create_dict)
        test_model.save()
        res = MyModel.query('a', MyModel.SK.eq('b'))
        assert next(res).to_dict() == {
            'ID': 'a',
            'PK': 'a',
            'SK': 'b',
            'Type': 'test_nested_map',
            'outer_map': {
                'mid_map': {
                    'foo': 'bar',
                },
            },
            'outer_list': [
                {'mid_map': {'baz': 'qux'}},
            ]
        }

    def test_nested_maps_with_list(self):
        class OuterMapAttribute(MapAttribute):
            mid_map = MapAttribute()

        class MyModel(Model):
            class Meta(Model.Meta):
                table_name = 'falcano-map-attr-e2e'
                billing_mode = 'PAY_PER_REQUEST'
            PK = UnicodeAttribute(hash_key=True)
            SK = UnicodeAttribute(range_key=True)
            outer_map = OuterMapAttribute()
            outer_list = ListAttribute()
            Type = UnicodeAttribute(default='test_nested_map')

        create_dict = {
            'PK': 'a',
            'SK': 'b',
            'outer_map': {
                'mid_map': {
                    'foo': ['bar'],
                },
            },
            'outer_list': [
                OuterMapAttribute(**{'mid_map': {'baz': 'qux'}}),
            ]
        }
        test_model = MyModel(**create_dict)
        test_model.save()
        res = MyModel.query('a', MyModel.SK.eq('b'))
        assert next(res).to_dict() == {
            'ID': 'a',
            'PK': 'a',
            'SK': 'b',
            'Type': 'test_nested_map',
            'outer_map': {
                'mid_map': {
                    'foo': ['bar'],
                },
            },
            'outer_list': [
                {'mid_map': {'baz': 'qux'}},
            ]
        }
