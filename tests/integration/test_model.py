import unittest
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError
import pytest
from _pytest.monkeypatch import MonkeyPatch
from falcano.model import Model
from falcano.attributes import (
    MapAttribute, UnicodeAttribute,
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
    Name = UnicodeAttribute(null=True)
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
    MapAttr = MapAttribute(null=True)

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
        self.friend2 = FriendModel('friend#jberk', 'friend#meta', Name='Justin Berk')
        self.friend2.save()
        self.friend3 = FriendModel('friend#fbladilsh', 'friend#meta', Name='Frank Bladilsh')
        self.friend3.save()

        self.group1 = FriendGroup('group#group1', 'group#meta', Name='Friendship Squad')
        self.group1.save()

        self.friend_to_update = FriendToUpdate(
            'update#first', 'update#meta', NumberAttr=2,
            SetAttr={'A', 'B'}, ListAttr=['One', 'Two'], StringAttr='First', MapAttr={'init': 'value'})
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

    def test_get(self):
        res = BaseModel.get(self.friend1.PK, self.friend1.SK)
        assert res.PK == self.friend1.PK
        assert res.SK == self.friend1.SK

    def test_batch_get(self):
        items = [
            (self.friend1.PK, self.friend1.SK),
            (self.group1.PK, self.group1.SK),
            (self.friend_to_update.PK, self.friend_to_update.SK)
        ]
        records = BaseModel.batch_get(items)
        records = records.records
        assert records[0].PK == self.group1.PK
        assert records[0].SK == self.group1.SK
        assert records[1].PK == self.friend1.PK
        assert records[1].SK == self.friend1.SK
        assert records[2].PK == self.friend_to_update.PK
        assert records[2].SK == self.friend_to_update.SK


    def test_update(self):
        expected = {
            'ID': 'first',
            'ListAttr': ['One', 'Two', 'three', 'four'],
            'NumberAttr': Decimal('-3'),
            'PK': 'update#first',
            'SK': 'update#meta',
            'SetAttr': {'Alphabet', 'A', 'B'},
            'Type': 'update_friend',
            'MapAttr': {'test': 'ok'}
        }
        self.friend_to_update.update(actions=[
            FriendToUpdate.NumberAttr.set(FriendToUpdate.NumberAttr - 5),
            FriendToUpdate.SetAttr.add({'Alphabet'}),
            FriendToUpdate.StringAttr.remove(),
            FriendToUpdate.ListAttr.set(FriendToUpdate.ListAttr.append(['three', 'four'])),
            FriendToUpdate.MapAttr.set({'test':'ok'})
        ])
        got = BaseModel.get(
            self.friend_to_update.PK,
            self.friend_to_update.SK).to_dict()
        assert expected == got

    def test_transact_write(self):
        new_friend = FriendModel('friend#new', 'friend#meta', Name='New Friend')
        with BaseModel.transact_write() as writer:
            writer.condition_check(FriendModel, 'friend#drue', 'friend#meta',
                                   FriendModel.Name.eq('Dan Rue'))
            writer.delete(self.friend2)
            writer.save(new_friend)
            actions = [FriendToUpdate.NumberAttr.add(5), FriendToUpdate.MapAttr.set({'test': 'ok'})]
            writer.update(self.friend_to_update, actions, condition=FriendToUpdate.NumberAttr.eq(2))

        with pytest.raises(Exception):
            BaseModel.get(self.friend2.PK, self.friend2.SK)
        BaseModel.get(new_friend.PK, new_friend.SK)
        assert self.friend_to_update.NumberAttr + \
            5 == BaseModel.get(self.friend_to_update.PK, self.friend_to_update.SK).NumberAttr
        assert BaseModel.get(self.friend_to_update.PK, self.friend_to_update.SK).MapAttr.as_dict() == {'test': 'ok'}

    def test_transact_get(self):
        want = self.friend1.to_dict()
        del want['CreatedAt']
        attributes_to_get = [
            FriendModel.PK.attr_name,
            FriendModel.SK.attr_name,
            FriendModel.Description.attr_name,
            FriendModel.Name.attr_name,
            FriendModel.Type.attr_name
        ]
        with BaseModel.transact_get() as getter:
            got_friend = getter.get(FriendModel, 'friend#drue', 'friend#meta', attributes_to_get)
        got = got_friend.get().to_dict()
        assert want == got

    def test_transact_get_with_map_and_list(self):
        want = self.friend_to_update.to_dict()

        with BaseModel.transact_get() as getter:
            got_friend_to_update = getter.get(FriendToUpdate, self.friend_to_update.PK, self.friend_to_update.SK)
        got = got_friend_to_update.get().to_dict()
        assert want == got

    def test_save_lists(self):
        thingy = FriendToUpdate(
            'update#first',
            'update#2',
            NumberAttr=2,
            SetAttr={'A', 'B'},
            ListAttr=['One', 2],
            StringAttr='First'
        )
        thingy.save()

        thingy = FriendToUpdate.get(
            'update#first',
            'update#2'
        )
        thingy.save()

        thingy = FriendToUpdate.get(
            'update#first',
            'update#2'
        )
        assert thingy.ListAttr == ['One', 2]
