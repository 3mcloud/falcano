import unittest
from datetime import datetime
from botocore.exceptions import ClientError
import pytest
from _pytest.monkeypatch import MonkeyPatch
from falcano.model import Model
from falcano.attributes import (
    UnicodeAttribute,
    UTCDateTimeAttribute,
    NumberAttribute,
    ListAttribute,
    UnicodeSetAttribute
)
from falcano.indexes import GlobalSecondaryIndex, AllProjection


class TypeIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'Type'
        billing_mode = 'PAY_PER_REQUEST'
        projection = AllProjection()
    Type = UnicodeAttribute(default='project', hash_key=True)
    SK = UnicodeAttribute(range_key=True)


class BaseModel(Model):
    '''Base model with meta'''
    class Meta(Model.Meta):
        table_name = 'falcano-e2e'
        billing_mode = 'PAY_PER_REQUEST'
    PK = UnicodeAttribute(hash_key=True)
    SK = UnicodeAttribute(range_key=True)
    TypeIndex = TypeIndex()


class FriendModel(BaseModel):
    """
    A model for testing
    """
    Type = UnicodeAttribute(default='friend')
    Name = UnicodeAttribute()
    Description = UnicodeAttribute(null=True)
    CreatedAt = UTCDateTimeAttribute(default=datetime.utcnow())


class FriendGroup(BaseModel):
    '''
    A model for a friendgroup
    '''
    Type = UnicodeAttribute(default='friend_group')
    Name = UnicodeAttribute(null=True)


class FriendToUpdate(BaseModel):
    '''
    A model for a friend that has lots of fun things to update
    '''
    Type = UnicodeAttribute(default='update_friend')
    NumberAttr = NumberAttribute(null=True)
    SetAttr = UnicodeSetAttribute(null=True)
    ListAttr = ListAttribute(null=True)
    StringAttr = UnicodeAttribute(null=True)


class TestModel(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fakeMyKeyId')
        self.monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fakeMySecret')
        if not BaseModel.exists():
            print('Creating table')
            FriendModel.create_table(wait=True)

        for item in BaseModel.scan():
            # clean up all items in db
            item.delete()

        self.friend1 = FriendModel('friend#drue', 'friend#meta',
                                   Name='Dan Rue', CreatedAt=datetime(2014, 5, 12, 23, 30))
        self.friend1.save()
        friend2 = FriendModel('friend#jberk', 'friend#meta', Name='Justin Berk')
        friend2.save()
        friend3 = FriendModel('friend#fbladilsh', 'friend#meta', Name='Frank Bladilsh')
        friend3.save()

        self.group1 = FriendGroup('group#group1', 'group#meta', Name='Friendship Squad')
        self.group1.save()

        self.friend_to_update = FriendToUpdate(
            'update#first', 'update#meta', NumberAttr=2,
            SetAttr={'A', 'B'}, ListAttr=['One', 'Two'], StringAttr='First')
        self.friend_to_update.save()

    def tearDown(self):
        # clean up all items in db
        for item in BaseModel.scan():
            item.delete()

    def test_existence(self):

        friend_group1 = FriendGroup(
            self.group1.PK,
            self.friend1.PK,
            Name="Boston"
        )
        friend_group1.save(FriendGroup.SK.does_not_exist())
        with pytest.raises(ClientError) as err:
            friend_group1.save(FriendGroup.SK.does_not_exist())
        assert err.typename == 'ConditionalCheckFailedException'

        friend_group1.Name = 'Seattle'
        res = friend_group1.save(FriendGroup.SK.exists())

        res = FriendGroup.query(
            self.group1.PK,
            FriendGroup.SK.startswith(
                self.friend1.PK
            )
        )
        assert list(res)[0].Name == 'Seattle'
        assert [friend.PK for friend in list(FriendModel.TypeIndex.query('friend'))] == \
            ['friend#drue', 'friend#fbladilsh', 'friend#jberk']

        for group in FriendGroup.query('group#group1', FriendGroup.SK.startswith('group#meta')):
            assert group.SK == 'group#meta'

    def test_time_storage(self):
        assert self.friend1.CreatedAt == datetime(2014, 5, 12, 23, 30)

    def test_update(self):
        expected = {'Attributes': {
            'ListAttr': ['One', 'Two', 'three', 'four'],
            'NumberAttr': -3,
            'PK': 'update#first',
            'SK': 'update#meta',
            'SetAttr': {'Alphabet', 'B', 'A'},
            'Type': 'update_friend'}
        }
        self.friend_to_update.update(actions=[
            FriendToUpdate.NumberAttr.set(FriendToUpdate.NumberAttr - 5),
            FriendToUpdate.SetAttr.add({'Alphabet'}),
            FriendToUpdate.StringAttr.remove(),
            FriendToUpdate.ListAttr.set(FriendToUpdate.ListAttr.append(['three', 'four']))
        ])
        res = list(BaseModel.query(
            self.friend_to_update.PK,
            FriendToUpdate.SK.eq('update#meta')))[0]._serialize()
        assert res == expected
