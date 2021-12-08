import os
import unittest
import pytest
from unittest import mock
import datetime
import decimal
from falcano.model import Model, _ModelFuture
from falcano.indexes import GlobalSecondaryIndex, AllProjection
from falcano.attributes import UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute, ListAttribute, MapAttribute
from falcano.exceptions import DoesNotExist
from _pytest.monkeypatch import MonkeyPatch

class TypeIndex(GlobalSecondaryIndex):
    """ Type Index """
    class Meta: # pylint: disable=too-few-public-methods
        """ GSI properties """
        index_name = 'Type'
        projection = AllProjection()
    Type = UnicodeAttribute(default='person', hash_key=True)
    SK = UnicodeAttribute(range_key=True)


class BaseModel(Model):
    '''Base model with meta'''
    class Meta(Model.Meta):
        ''' Table properties '''
        table_name = os.environ.get('DYNAMODB_TABLE')
    PK = UnicodeAttribute(hash_key=True)
    SK = UnicodeAttribute(range_key=True)
    TypeIndex = TypeIndex()


class Person(BaseModel):
    Type = UnicodeAttribute(default='person')
    FirstName = UnicodeAttribute()
    LastName = UnicodeAttribute()
    Age = NumberAttribute(default=0)
    CreateDate = UTCDateTimeAttribute(attr_name='CreateDateTime')
    ValueList = ListAttribute()
    ValueMap = MapAttribute()
    DoesNotExist = DoesNotExist


class TestPaginator(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fakeMyKeyId')
        self.monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fakeMySecret')

        primary = 0
        secondary = 2000
        lst_persons = []

        while primary < 1001:
            person = Person(
                f"person#{primary}",
                f"person#{secondary}",
                FirstName="Rick",
                LastName="Sanchez",
                Age=70,
                ValueList=[1,'2'],
                ValueMap={'test': 'ok'}
            )
            
            lst_persons.append(person)
            primary+=1
            secondary+=1

        Person.create_table()
        with Person.batch_write() as batch:
            for person in lst_persons:
                print(person.PK)
                batch.save(person)

    def tearDown(self):
        with Person.batch_write() as batch:
            for item in Person.scan():
                batch.delete(item)

    def test_paginator_on_thousand_plus_data(self):
        try: 
            Person.scan().collection()
        except RecursionError:
            pytest.fail("Paginator test failed on 1000+ data")
        # print(collection)




    # def test_reset():
    #     reset_person = Person.scan().reset()
    #     assert reset_person.__index == 0
    # make unit target=/test_paginator.py::test_a_thing

    #learn about conftest.py, use the setup and teardown methods to clean up the table

        # rick = Person(
    #     "person#1234",
    #     "person#ricksanchez",
    #     FirstName="Rick",
    #     LastName="Sanchez",
    #     Age=70,
    #     ValueList=[1,'2'],
    #     ValueMap={'test': 'ok'}
    # )

    # morty = Person(
    #     "person#4321",
    #     "person#mortyperson",
    #     FirstName="Morty",
    #     LastName="Person",
    #     Age=8,
    #     ValueList=[1,'a'],
    #     ValueMap={'something': 'valid'}
    # )

    # test that a 1000++ fails on recursion and works on iteration 

    # test to_models and reset

    # write test to cover for 100% coverage 

    #rick.save()
    #morty.save()