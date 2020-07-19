from unittest import TestCase
from datetime import datetime
import pytest
from falcano.model import Model
from falcano.attributes import UnicodeAttribute
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

class FriendGroup(BaseModel):
    '''
    A model for a friendgroup
    '''
    Type = UnicodeAttribute(default='friend_group')

class GroupModel(BaseModel):
    """
    A model for groups
    """
    # PK: group id
    # SK: friend id
    Type = UnicodeAttribute(default='group')
    Name = UnicodeAttribute()


def test_model_integration(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fakeMyKeyId')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fakeMySecret')

    if not FriendModel.exists():
        print('Creating table')
        FriendModel.create_table(wait=True)


    friend1 = FriendModel('friend#drue', 'friend#meta', Name='Dan Rue')
    friend1.save()
    friend2 = FriendModel('friend#jberk', 'friend#meta', Name='Justin Berk')
    friend2.save()
    friend3 = FriendModel('friend#fbladilsh', 'friend#meta', Name='Frank Bladilsh')
    friend3.save()

    group1 = GroupModel('group#group1', 'group#meta', Name='Best Friends')
    group1.save()

    friend_group1 = FriendGroup(group1.PK, friend1.PK)
    # friend_group1.save(FriendGroup.SK.does_not_exist)

    assert [friend.PK for friend in list(FriendModel.TypeIndex.query('friend'))] == \
        ['friend#drue', 'friend#fbladilsh', 'friend#jberk']

    for group in GroupModel.query('group#group1', GroupModel.SK.startswith('group#meta')):
        assert group.SK == 'group#meta'

    for thingy in FriendModel.scan():
        thingy.delete()

    assert [friend.PK for friend in list(FriendModel.scan())] == []
