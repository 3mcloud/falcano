import unittest
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError
import pytest
from _pytest.monkeypatch import MonkeyPatch
from falcano.model import Model
from falcano.attributes import (
    UnicodeAttribute,
    UTCDateTimeAttribute,
    NumberAttribute,
    ListAttribute,
    UnicodeSetAttribute,
    MapAttribute,
)
from falcano.indexes import GlobalSecondaryIndex, AllProjection


class TestModel(Model):
    '''Test model with meta'''
    class Meta(Model.Meta):
        table_name = 'falcano-e2e'
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
