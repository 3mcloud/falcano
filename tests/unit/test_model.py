import os
import pytest
from unittest import mock
from falcano.model import Model
from falcano.indexes import GlobalSecondaryIndex, AllProjection
from falcano.attributes import UnicodeAttribute, NumberAttribute
from falcano.exceptions import DoesNotExist


class TypeIndex(GlobalSecondaryIndex):
    """ Type Index """
    class Meta: # pylint: disable=too-few-public-methods
        """ GSI properties """
        index_name = 'Type'
        projection = AllProjection()
    Type = UnicodeAttribute(default='person', hash_key=True)
    sk = UnicodeAttribute(range_key=True)


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
    Age = NumberAttribute()
    DoesNotExist = DoesNotExist


rick = Person(
    "person#1234",
    "person#ricksanchez",
    FirstName="Rick",
    LastName="Sanchez",
    Age=70,
)

morty = Person(
    "person#5678",
    "person#mortysmith",
    FirstName="Morty",
    LastName="Smith",
    Age=14,
)

summer = Person(
    "person#9999",
    "person#summersmith",
    FirstName="Summer",
    LastName="Smith",
    Age=18,
)

jerry = Person(
    "person#8888",
    "person#jerrysmith",
    FirstName="Jerry",
    LastName="Smith",
    Age=35,
)

@pytest.fixture
def mock_dynamodb():
    mock_resource = mock.Mock()
    mock_resource.return_value = mock_resource
    mock_table = mock.Mock()
    mock_resource.Table.return_value = mock_table
    return mock_resource, mock_table


@pytest.fixture
def mock_environ():
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['DYNAMODB_TABLE'] = 'character-table'


def test_save(mock_environ, mock_dynamodb):
    mock_resource, mock_table = mock_dynamodb
    Person.resource = mock_resource

    rick.save()
    mock_table.put_item.assert_called_with(
        TableName=None,
        Item={
            'Age': 70,
            'FirstName': 'Rick',
            'LastName': 'Sanchez',
            'PK': 'person#1234',
            'SK': 'person#ricksanchez',
            'Type': 'person'},
        ReturnValues='NONE'
    )

    morty.save(condition=Person.FirstName.eq("Morty"))
    mock_table.put_item.assert_called_with(
        TableName=None,
        Item={
            'Age': 14,
            'FirstName': 'Morty',
            'LastName': 'Smith',
            'PK': 'person#5678',
            'SK': 'person#mortysmith',
            'Type': 'person'
        },
        ConditionExpression='#n0 = :v0',
        ReturnValues='NONE',
        ExpressionAttributeNames={'#n0': 'FirstName'},
        ExpressionAttributeValues={':v0': 'Morty'}
    )

    summer.save(return_values='UPDATED_NEW')
    mock_table.put_item.assert_called_with(
        TableName=None,
        Item={
            'Age': 18,
            'FirstName': 'Summer',
            'LastName': 'Smith',
            'PK': 'person#9999',
            'SK': 'person#summersmith',
            'Type': 'person'
        },
        ReturnValues='UPDATED_NEW'
    )

    jerry.save(return_values='ALL_NEW', condition=Person.Age.eq(35))
    mock_table.put_item.assert_called_with(
        TableName=None,
        Item={
            'Age': 35,
            'FirstName': 'Jerry',
            'LastName': 'Smith',
            'PK': 'person#8888',
            'SK': 'person#jerrysmith',
            'Type': 'person'
        },
        ConditionExpression='#n0 = :v0',
        ReturnValues='ALL_NEW',
        ExpressionAttributeNames={'#n0': 'Age'},
        ExpressionAttributeValues={':v0': 35}
    )


def test_update(mock_environ, mock_dynamodb):
    mock_resource, mock_table = mock_dynamodb
    Person.resource = mock_resource

    mock_table.update_item.return_value = {
        'Attributes': {
            'Age': 71,
            'FirstName': 'Rick',
            'LastName': 'Sanchez',
            'PK': 'person#1234',
            'SK': 'person#ricksanchez',
            'Type': 'person'
        }
    }
    rick.update(actions=[Person.Age.set(Person.Age + 1)])
    mock_table.update_item.assert_called_with(
        TableName=None,
        Key={'PK': 'person#1234', 'SK': 'person#ricksanchez'},
        ReturnValues='ALL_NEW',
        UpdateExpression='SET #0 = #0 + :0',
        ExpressionAttributeNames={'#0': 'Age'},
        ExpressionAttributeValues={':0': 1}
    )

    mock_table.update_item.return_value = {
        'Attributes': {
            'Age': 14,
            'FirstName': 'The One True Morty',
            'LastName': 'Smith',
            'PK': 'person#5678',
            'SK': 'person#theonetruemortysmith',
            'Type': 'person'
        }
    }
    morty.update(
        actions=[Person.FirstName.set('The One True Morty')],
        condition=Person.FirstName.eq("Morty")
    )
    mock_table.update_item.assert_called_with(
        TableName=None,
        Key={'PK': 'person#5678', 'SK': 'person#mortysmith'},
        ConditionExpression='#n0 = :v0',
        ReturnValues='ALL_NEW',
        UpdateExpression='SET #n0 = :1',
        ExpressionAttributeNames={'#n0': 'FirstName'},
        ExpressionAttributeValues={':v0': 'Morty', ':1': 'The One True Morty'}
    )

    mock_table.update_item.return_value = {
        'Attributes': {
            'Age': 18,
            'FirstName': 'Summer',
            'LastName': 'Palicky',
            'PK': 'person#9999',
            'SK': 'person#summerpalicky',
            'Type': 'person'
        }
    }
    summer.update(
        actions=[Person.LastName.set('Palicky')],
        return_values='UPDATED_NEW'
    )
    mock_table.update_item.assert_called_with(
        TableName=None,
        Key={'PK': 'person#9999', 'SK': 'person#summersmith'},
        ReturnValues='UPDATED_NEW',
        UpdateExpression='SET #0 = :0',
        ExpressionAttributeNames={'#0': 'LastName'},
        ExpressionAttributeValues={':0': 'Palicky'}
    )

    mock_table.update_item.return_value = {
        'Attributes': {
            'Age': 35,
            'FirstName': 'Doofus',
            'LastName': 'Smith',
            'PK': 'person#8888',
            'SK': 'person#doofussmith',
            'Type': 'person'
        }
    }
    jerry.update(
        actions=[Person.FirstName.set('Doofus')],
        return_values='ALL_NEW',
        condition=Person.FirstName.eq('Jerry')
    )
    mock_table.update_item.assert_called_with(
        TableName=None,
        Key={'PK': 'person#8888', 'SK': 'person#jerrysmith'},
        ConditionExpression='#n0 = :v0',
        ReturnValues='ALL_NEW',
        UpdateExpression='SET #n0 = :1',
        ExpressionAttributeNames={'#n0': 'FirstName'},
        ExpressionAttributeValues={':v0': 'Jerry', ':1': 'Doofus'}
    )


def test_delete(mock_environ, mock_dynamodb):
    mock_resource, mock_table = mock_dynamodb
    Person.resource = mock_resource

    rick.delete()
    mock_table.delete_item.assert_called_with(
        TableName=None, Key={'PK': 'person#1234', 'SK': 'person#ricksanchez'}, ReturnValues='NONE'
    )

    morty.delete(condition=Person.FirstName.eq("Morty"))
    mock_table.delete_item.assert_called_with(
        TableName=None,
        Key={'PK': 'person#5678', 'SK': 'person#mortysmith'},
        ReturnValues='NONE',
        ConditionExpression='#n0 = :v0',
        ExpressionAttributeNames={'#n0': 'FirstName'},
        ExpressionAttributeValues={':v0': 'Morty'}
    )

    summer.delete(return_values='UPDATED_NEW')
    mock_table.delete_item.assert_called_with(
        TableName=None,
        Key={'PK': 'person#9999', 'SK': 'person#summersmith'},
        ReturnValues='UPDATED_NEW',
    )

    jerry.delete(return_values='ALL_NEW', condition=Person.Age.eq(35))
    mock_table.delete_item.assert_called_with(
        TableName=None,
        Key={'PK': 'person#8888', 'SK': 'person#jerrysmith'},
        ConditionExpression='#n0 = :v0',
        ReturnValues='ALL_NEW',
        ExpressionAttributeNames={'#n0': 'Age'},
        ExpressionAttributeValues={':v0': 35}
    )

