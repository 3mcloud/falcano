import os
import unittest
import pytest
import sys
from falcano.model import Model
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

class SmallPerson(BaseModel):
    Type = UnicodeAttribute(default='person')
    FirstName = UnicodeAttribute()
    LastName = UnicodeAttribute()
    Age = NumberAttribute(default=0)
    CreateDate = UTCDateTimeAttribute(attr_name='CreateDateTime')
    ValueList = ListAttribute()
    ValueMap = MapAttribute()
    DoesNotExist = DoesNotExist


class TestPaginator(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fakeMyKeyId')
        self.monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fakeMySecret')

        primary = 0
        secondary = 2000
        person_lst = []
        small_person_lst = []

        sys.setrecursionlimit(100)

        while primary < 101:
            person = Person(
                f"person#{primary}",
                f"person#{secondary}",
                FirstName="Rick",
                LastName="Sanchez",
                Age=70,
                ValueList=[1,'2'],
                ValueMap={'test': 'ok'}
            )
            
            if primary < 5: small_person_lst.append(person) #random small number
            person_lst.append(person)

            primary+=1
            secondary+=1

        Person.create_table()
        with Person.batch_write() as batch:
            for person in person_lst:
                batch.save(person)
        
        SmallPerson.create_table()
        with SmallPerson.batch_write() as batch:
            for person in small_person_lst:
                batch.save(person)

    @classmethod
    def tearDownClass(self):
        with Person.batch_write() as batch:
            for object in Person.scan():
                batch.delete(object)

        with SmallPerson.batch_write() as batch:
            for object in SmallPerson.scan():
                batch.delete(object)
        
        sys.setrecursionlimit(1000)


    def test_paginator_for_recursion_depth(self):
        try: 
            Person.scan().collection()
        except RecursionError:
            pytest.fail("Paginator test failed on 1000+ data")


    def test_reset(self):
        results = SmallPerson.scan()
        person = next(iter(results))
        person2 = next(iter(results))
        results.reset()
        assert person.to_dict() == next(iter(results)).to_dict()
        assert person2.to_dict() == next(iter(results)).to_dict()
        