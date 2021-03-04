import os
import pytest
from unittest import mock
import datetime
import decimal
from falcano.model import Model, _ModelFuture
from falcano.indexes import GlobalSecondaryIndex, AllProjection
from falcano.attributes import UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute, ListAttribute, MapAttribute
from falcano.exceptions import DoesNotExist


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


rick = Person(
    "person#1234",
    "person#ricksanchez",
    FirstName="Rick",
    LastName="Sanchez",
    Age=70,
    ValueList=[1,'2'],
    ValueMap={'test': 'ok'}
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

bird_person = Person(
    "person#7777",
    "person#birdperson",
    FirstName="Bird",
    LastName="Person",
)


@pytest.fixture
def mock_table_resource():
    mock_resource = mock.Mock()
    mock_resource.return_value = mock_resource
    mock_table = mock.Mock()
    mock_resource.Table.return_value = mock_table
    return mock_resource, mock_table

@pytest.fixture
def mock_client():
    mock_client = mock.Mock()
    mock_client.return_value = mock_client
    return mock_client


def test_set_defaults():
    bird_person._set_defaults()
    bird_person_dict = bird_person.to_dict()
    assert bird_person_dict == {
        'Age': 0,
        'Type': 'person',
        'FirstName': 'Bird',
        'LastName': 'Person',
        'PK': 'person#7777',
        'SK': 'person#birdperson',
        'ID': '7777'
    }


def test_initialize_attributes():
    bird_person.initialize_attributes(True)
    bird_person_dict = bird_person.to_dict()
    assert bird_person_dict == {
        'Age': 0,
        'Type': 'person',
        'FirstName': 'Bird',
        'LastName': 'Person',
        'PK': 'person#7777',
        'SK': 'person#birdperson',
        'ID': '7777'
    }


def test_get_attributes():
    attributes = Person.get_attributes()
    att_types = {
        UnicodeAttribute: ['PK', 'SK', 'FirstName', 'LastName', 'Type'],
        NumberAttribute: ['Age'],
        UTCDateTimeAttribute: ['CreateDate'],
        MapAttribute: ['ValueMap'],
        ListAttribute: ['ValueList']
    }
    assert len(attributes) == 9
    for att in attributes:
        assert att[0] in att_types[type(att[1])]


def test_get_attribute():
    members = ['PK', 'SK', 'FirstName', 'LastName', 'Age', 'Type']
    for member in members:
        attribute = Person.get_attribute(member)
        if member == 'Age':
            assert isinstance(attribute, NumberAttribute)
        else:
            assert isinstance(attribute, UnicodeAttribute)


def test_get_hash_key():
    hash_key = Person.get_hash_key()
    assert isinstance(hash_key, UnicodeAttribute)
    assert hash_key.is_hash_key


def test_get_range_key():
    range_key = Person.get_range_key()
    assert isinstance(range_key, UnicodeAttribute)
    assert range_key.is_range_key


def test_get_indexes():
    indexes = Person._get_indexes()
    assert 'AttributeDefinitions' in indexes
    assert len(indexes['AttributeDefinitions']) == 2
    for att_def in indexes['AttributeDefinitions']:
        assert att_def['AttributeType'] == 'S'
        assert att_def['AttributeName'] in ['SK', 'Type']
    assert len(indexes['GlobalSecondaryIndexes']) == 1
    gsi = indexes['GlobalSecondaryIndexes'][0]
    assert gsi['IndexName'] == 'Type'
    assert gsi['Projection']['ProjectionType'] == 'ALL'
    assert len(gsi['KeySchema']) == 2
    for key in gsi['KeySchema']:
        assert key['AttributeName'] in ['SK', 'Type']
        if key['AttributeName'] == 'SK':
            assert key['KeyType'] == 'RANGE'
        else:
            assert key['KeyType'] == 'HASH'


def test_get_schema():
    schema = Person.get_schema()
    assert 'AttributeDefinitions' in schema
    assert len(schema['AttributeDefinitions']) == 2
    for att_def in schema['AttributeDefinitions']:
        assert att_def['AttributeType'] == 'S'
        assert att_def['AttributeName'] in ['PK', 'SK']
    assert len(schema['KeySchema']) == 2
    for key in schema['KeySchema']:
        assert key['AttributeName'] in ['PK', 'SK']
        if key['AttributeName'] == 'SK':
            assert key['KeyType'] == 'RANGE'
        else:
            assert key['KeyType'] == 'HASH'
    assert schema['TableName'] == 'unit-test-table'
    assert schema['BillingMode'] == 'PAY_PER_REQUEST'


def test_create_table(mock_client):
    Person.connection = mock_client
    Person.create_table()
    mock_client.create_table.assert_called_with(
        AttributeDefinitions=[
            {'AttributeName': 'PK', 'AttributeType': 'S'},
            {'AttributeName': 'SK', 'AttributeType': 'S'},
            {'AttributeName': 'Type', 'AttributeType': 'S'}],
        KeySchema=[{'KeyType': 'HASH', 'AttributeName': 'PK'}, {'KeyType': 'RANGE', 'AttributeName': 'SK'}],
        TableName='unit-test-table',
        BillingMode='PAY_PER_REQUEST',
        GlobalSecondaryIndexes=[
            {'IndexName': 'Type', 'KeySchema': [{'AttributeName': 'Type', 'KeyType': 'HASH'}, {'AttributeName': 'SK', 'KeyType': 'RANGE'}], 'Projection': {'ProjectionType': 'ALL'}}
        ]
    )


def test_scan(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
    Person.resource = mock_resource
    mock_table.scan.return_value = {
        'Items': []
    }
    Person.scan()
    mock_table.scan.assert_called_with(
        TableName='unit-test-table'
    )
    Person.scan(
        select='ALL_ATTRIBUTES',
        limit=200,
        attributes_to_get=['FirstName', 'LastName'],
        consistent_read=True,
        condition=Person.FirstName.eq('Morty'),
        filter_condition=Person.SK.startswith('person')
    )

    mock_table.scan.assert_called_with(
        TableName='unit-test-table',
        ConditionExpression='#n0 = :v0',
        FilterExpression=Person.SK.startswith('person'),
        ConsistentRead=True,
        Limit=200,
        ProjectionExpression='#n0, #1',
        Select='ALL_ATTRIBUTES',
        ExpressionAttributeNames={'#n0': 'FirstName', '#1': 'LastName'},
        ExpressionAttributeValues={':v0': 'Morty'}
    )


def test_get(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
    Person.resource = mock_resource
    mock_table.get_item.return_value = {
        'Item': {
            'FirstName': 'Summer',
            'LastName': 'Smith',
            'Age': 18,
            'Type': 'person'
        }
    }
    Person.get(hash_key="person#9999", range_key="person#summersmith")
    mock_table.get_item.assert_called_with(
        TableName='unit-test-table',
        Key={'PK': 'person#9999', 'SK': 'person#summersmith'}
    )

    Person.get(
        hash_key="person#9999",
        range_key="person#summersmith",
        attributes_to_get=['FirstName', 'LastName'],
        consistent_read=True
    )
    mock_table.get_item.assert_called_with(
        TableName='unit-test-table',
        Key={'PK': 'person#9999', 'SK': 'person#summersmith'},
        ConsistentRead=True,
        ProjectionExpression='#0, #1',
        ExpressionAttributeNames={'#0': 'FirstName', '#1': 'LastName'})


def test_batch_get(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
    Person.resource = mock_resource
    mock_resource.batch_get_item.return_value = {
        'Responses': {
            'unit-test-table': [
                {
                    'Age': 71,
                    'FirstName': 'Rick',
                    'LastName': 'Sanchez',
                    'PK': 'person#1234',
                    'SK': 'person#ricksanchez',
                    'Type': 'person'
                },
                {
                    'Age': 14,
                    'FirstName': 'Morty',
                    'LastName': 'Smith',
                    'PK': 'person#5678',
                    'SK': 'person#mortysmith',
                    'Type': 'person'
                }
            ]
        },
        'UnprocessedKeys': {
            'unit-test-table': {

            }
        }
    }
    items = [
        ("person#1234", "person#ricksanchez"),
        ("person#5678", "person#mortysmith"),
    ]

    Person.batch_get(items)
    mock_resource.batch_get_item.assert_called_with(
        RequestItems={
            'unit-test-table': {
                'Keys': [
                    {'PK': 'person#5678', 'SK': 'person#mortysmith'},
                    {'PK': 'person#1234', 'SK': 'person#ricksanchez'}
                ]
            }
        }
    )

    items = [
        ("person#8888", "person#jerrysmith"),
        ("person#9999", "person#summersmith"),
    ]

    Person.batch_get(items=items, ConsistentRead=True, AttributesToGet=['FirstName', 'LastName'])
    mock_resource.batch_get_item.assert_called_with(
        RequestItems={'unit-test-table': {'ConsistentRead': True, 'AttributesToGet': ['FirstName', 'LastName'], 'Keys': [{'PK': 'person#9999', 'SK': 'person#summersmith'}, {'PK': 'person#8888', 'SK': 'person#jerrysmith'}]}}
    )


def test_batch_get_page(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
    Person.resource = mock_resource
    mock_resource.batch_get_item.return_value = {
        'Responses': {
            'unit-test-table': [
                {
                    'Age': 71,
                    'FirstName': 'Rick',
                    'LastName': 'Sanchez',
                    'PK': 'person#1234',
                    'SK': 'person#ricksanchez',
                    'Type': 'person'
                },
                {
                    'Age': 14,
                    'FirstName': 'Morty',
                    'LastName': 'Smith',
                    'PK': 'person#5678',
                    'SK': 'person#mortysmith',
                    'Type': 'person'
                }
            ]
        },
        'UnprocessedKeys': {
            'unit-test-table': {

            }
        }
    }

    keys_to_get = [{'PK': 'person#5678', 'SK': 'person#mortysmith'}, {'PK': 'person#1234', 'SK': 'person#ricksanchez'}]
    Person._batch_get_page(keys_to_get)
    mock_resource.batch_get_item.assert_called_with(
        RequestItems={
            'unit-test-table': {
                'Keys': [
                    {'PK': 'person#5678', 'SK': 'person#mortysmith'},
                    {'PK': 'person#1234', 'SK': 'person#ricksanchez'}
                ]
            }
        }
    )


def test_exists(mock_client):
    Person.connection = mock_client
    mock_client.describe_table.retuen_value = {
        'Table': {
            'TableName': 'unit-test-table'
        }
    }
    assert Person.exists()


def test_query(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
    Person.resource = mock_resource
    mock_table.query.return_value = {
        'Items': [
            {
                'Age': 70,
                'FirstName': 'Rick',
                'LastName': 'Sanchez',
                'PK': 'person#1234',
                'SK': 'person#ricksanchez',
                'Type': 'person'
            }
        ]
    }
    Person.query(rick.PK)
    mock_table.query.assert_called_with(
        TableName='unit-test-table', KeyConditionExpression=Person.get_hash_key().eq(rick.PK), ConsistentRead=False
    )

    Person.query(
        morty.PK,
        range_key_condition=Person.SK.startswith("person"),
        filter_condition=Person.LastName.eq("Smith"),
        consistent_read=True,
        limit=1,
        attributes_to_get=['FirstName', 'LastName'],
        page_size=20
    )
    mock_table.query.assert_called_with(
        TableName='unit-test-table', KeyConditionExpression=Person.get_hash_key().eq(morty.PK) & Person.SK.startswith("person"),
        FilterExpression=Person.LastName.eq("Smith"), ConsistentRead=True, Limit=20, ProjectionExpression='#0, #1',
        Select='SPECIFIC_ATTRIBUTES', ExpressionAttributeNames={'#0': 'FirstName', '#1': 'LastName'}
    )


def test_serialize():
    attrs = rick.serialize()
    assert attrs['HASH'] == 'person#1234'
    assert attrs['RANGE'] == 'person#ricksanchez'
    assert attrs['Attributes']['FirstName'] == 'Rick'
    assert attrs['Attributes']['LastName'] == 'Sanchez'
    assert attrs['Attributes']['Age'] == 70
    assert attrs['Attributes']['PK'] == 'person#1234'
    assert attrs['Attributes']['SK'] == 'person#ricksanchez'
    assert attrs['Attributes']['Type'] == 'person'


def test_get_keys():
    keys = rick.get_keys()
    assert keys['PK'] == 'person#1234'
    assert keys['SK'] == 'person#ricksanchez'


def test_serialize_value():
    serialized = Person._serialize_value(Person.CreateDate, datetime.datetime(2020, 5, 17))
    assert serialized == '2020-05-17T00:00:00.000000+0000'


def test_serialize_keys():
    hash_key, range_key = Person._serialize_keys('person#1234')
    assert hash_key == 'person#1234'
    assert range_key is None
    hash_key, range_key = Person._serialize_keys('person#1234', 'person#ricksanchez')
    assert hash_key == 'person#1234'
    assert range_key == 'person#ricksanchez'


def test_from_raw_data():
    raw_data = {
        'Item': {
            'FirstName': 'Summer',
            'LastName': 'Smith',
            'Age': 18,
            'Type': 'person'
        }
    }
    result = Person.from_raw_data(raw_data)
    assert isinstance(result, Person)


def test_dynamo_to_python_attr():
    result = Person._dynamo_to_python_attr('CreateDateTime')
    assert result == 'CreateDate'


def test_transact_write(mock_client):
    Person.connection = mock_client
    with Person.transact_write() as writer:
        writer.save(rick)
        writer.save(morty)
        writer.delete(summer)
        writer.delete(bird_person)
    mock_client.transact_write_items.assert_called_with(
        TransactItems=[
            {
                'Delete': {
                    'TableName': 'unit-test-table',
                    'Key': {
                        'PK': {'S': 'person#9999'},
                        'SK': {'S': 'person#summersmith'}
                    }
                }
            },
            {
                'Delete': {
                    'TableName': 'unit-test-table',
                    'Key': {
                        'PK': {'S': 'person#7777'},
                        'SK': {'S': 'person#birdperson'}
                    }
                }
            },
            {
               'Put': {
                    'TableName': 'unit-test-table',
                    'Item': {
                        'PK': {'S': 'person#1234'},
                        'SK': {'S': 'person#ricksanchez'},
                        'Age': {'N': '70'},
                        'FirstName': {'S': 'Rick'},
                        'LastName': {'S': 'Sanchez'},
                        'Type': {'S': 'person'},
                        'ValueList': {'L': [{'N': '1'}, {'S': '2'}]},
                        'ValueMap': {'M': {'test': {'S': 'ok'}}}
                    }
               }
            },
            {
               'Put': {
                    'TableName': 'unit-test-table',
                    'Item': {
                        'PK': {'S': 'person#5678'},
                        'SK': {'S': 'person#mortysmith'},
                        'Age': {'N': '14'},
                        'FirstName': {'S': 'Morty'},
                        'LastName': {'S': 'Smith'},
                        'Type': {'S': 'person'}
                    }
               }
            }
        ]
    )


def test_transact_get(mock_client):
    Person.connection = mock_client
    mock_client.transact_get_items.return_value = {
        'Responses': [
            {
                'Item': {
                    'PK': {'S': 'person#1234'},
                    'SK': {'S': 'person#ricksanchez'},
                    'Age': {'N': '70'},
                    'FirstName': {'S': 'Rick'},
                    'LastName': {'S': 'Sanchez'},
                    'Type': {'S': 'person'}
                }
            },
            {
                'Item': {
                    'PK': {'S': 'person#5678'},
                    'SK': {'S': 'person#mortysmith'},
                    'Age': {'N': '14'},
                    'FirstName': {'S': 'Morty'},
                    'LastName': {'S': 'Smith'},
                    'Type': {'S': 'person'}
                }
            }
        ]
    }
    with Person.transact_get() as getter:
        got_rick = getter.get(Person, "person#1234", "person#ricksanchez")
        got_morty = getter.get(Person, "person#5678", "person#mortysmith")
    mock_client.transact_get_items.assert_called_with(
        TransactItems=[
            {
                'Get': {
                    'TableName': 'unit-test-table',
                    'Key': {
                        'PK': {'S': 'person#1234'},
                        'SK': {'S': 'person#ricksanchez'}
                    }
                }
            },
            {
                'Get': {
                    'TableName': 'unit-test-table',
                    'Key': {
                        'PK': {'S': 'person#5678'},
                        'SK': {'S': 'person#mortysmith'}
                    }
                }
            }
        ]
    )
    assert isinstance(got_rick, _ModelFuture)
    assert isinstance(got_morty, _ModelFuture)


def test_batch_write(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
    Person.resource = mock_resource
    batch_mock = mock.Mock()
    batch_writer_mock = mock.Mock()
    batch_writer_mock.__enter__ = mock.Mock(return_value=batch_mock)
    batch_writer_mock.__exit__ = mock.Mock(return_value=None)
    mock_table.batch_writer.return_value = batch_writer_mock
    batch_writer = Person.batch_write()
    batch_writer.save(
        Person(
            'person#1234',
            'person#ricksanchez',
            Age=70,
            FirstName='Rick',
            LastName='Sanchez',
            Type='person'
        )
    )
    batch_writer.delete(
        Person(
            'person#7777',
            'person#birdperson',
            Age=0,
            FirstName='Bird',
            LastName='Person',
            Type='person'
        )
    )
    assert len(batch_writer.pending_operations) == 2
    for pending_operation in batch_writer.pending_operations:
        assert 'Item' in pending_operation
        item = pending_operation['Item'].to_dict()
        assert'Action' in pending_operation
        assert pending_operation['Action'] in ['PUT', 'DELETE']
        if pending_operation['Action'] == 'PUT':
            assert item['FirstName'] == 'Rick'
            assert item['LastName'] == 'Sanchez'
            assert item['Age'] == 70
            assert item['PK'] == 'person#1234'
            assert item['SK'] == 'person#ricksanchez'
            assert item['Type'] == 'person'
        else:
            assert item['Type'] == 'person'
            assert item['FirstName'] == 'Bird'
            assert item['LastName'] == 'Person'
            assert item['Age'] == 0
            assert item['PK'] == 'person#7777'
            assert item['SK'] == 'person#birdperson'
            assert item['ID'] == '7777'

    batch_writer.commit()

    batch_mock.put_item.assert_called_with(
        Item={'Age': 70, 'FirstName': 'Rick', 'LastName': 'Sanchez', 'PK': 'person#1234', 'SK': 'person#ricksanchez', 'Type': 'person'}
    )

    batch_mock.delete_item.assert_called_with(
        Key={'PK': 'person#7777', 'SK': 'person#birdperson'}
    )


def test_save(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
    Person.resource = mock_resource

    rick.save()
    mock_table.put_item.assert_called_with(
        TableName='unit-test-table',
        Item={
            'Age': 70,
            'FirstName': 'Rick',
            'LastName': 'Sanchez',
            'PK': 'person#1234',
            'SK': 'person#ricksanchez',
            'Type': 'person',
            'ValueList': [1, '2'],
            'ValueMap': {'test': 'ok'}
        },
        ReturnValues='NONE'
    )

    morty.save(condition=Person.FirstName.eq("Morty"))
    mock_table.put_item.assert_called_with(
        TableName='unit-test-table',
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
        TableName='unit-test-table',
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
        TableName='unit-test-table',
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


def test_update(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
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
        TableName='unit-test-table',
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
        TableName='unit-test-table',
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
        TableName='unit-test-table',
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
        condition=Person.FirstName.eq('Jerry') & Person.SK.startswith('person')
    )
    mock_table.update_item.assert_called_with(
        TableName='unit-test-table',
        Key={'PK': 'person#8888', 'SK': 'person#jerrysmith'},
        ConditionExpression='(#n0 = :v0 AND begins_with(#n1, :v1))',
        ReturnValues='ALL_NEW', UpdateExpression='SET #n0 = :2',
        ExpressionAttributeNames={'#n0': 'FirstName', '#n1': 'SK'},
        ExpressionAttributeValues={':v0': 'Jerry', ':v1': 'person', ':2': 'Doofus'}
    )


def test_delete(mock_table_resource):
    mock_resource, mock_table = mock_table_resource
    Person.resource = mock_resource

    rick.delete()
    mock_table.delete_item.assert_called_with(
        TableName='unit-test-table', Key={'PK': 'person#1234', 'SK': 'person#ricksanchez'}, ReturnValues='NONE'
    )

    morty.delete(condition=Person.FirstName.eq("Morty"))
    mock_table.delete_item.assert_called_with(
        TableName='unit-test-table',
        Key={'PK': 'person#5678', 'SK': 'person#mortysmith'},
        ReturnValues='NONE',
        ConditionExpression='#n0 = :v0',
        ExpressionAttributeNames={'#n0': 'FirstName'},
        ExpressionAttributeValues={':v0': 'Morty'}
    )

    summer.delete(return_values='UPDATED_NEW')
    mock_table.delete_item.assert_called_with(
        TableName='unit-test-table',
        Key={'PK': 'person#9999', 'SK': 'person#summersmith'},
        ReturnValues='UPDATED_NEW',
    )

    jerry.delete(return_values='ALL_NEW', condition=Person.Age.eq(35))
    mock_table.delete_item.assert_called_with(
        TableName='unit-test-table',
        Key={'PK': 'person#8888', 'SK': 'person#jerrysmith'},
        ConditionExpression='#n0 = :v0',
        ReturnValues='ALL_NEW',
        ExpressionAttributeNames={'#n0': 'Age'},
        ExpressionAttributeValues={':v0': 35}
    )


def test_to_dict():
    rick_dict = rick.to_dict()
    assert rick_dict['FirstName'] == 'Rick'
    assert rick_dict['LastName'] == 'Sanchez'
    assert rick_dict['Age'] == 70
    assert rick_dict['PK'] == 'person#1234'
    assert rick_dict['SK'] == 'person#ricksanchez'
    assert rick_dict['Type'] == 'person'


def test_attr2obj():
    result = rick._attr2obj([decimal.Decimal('5'), decimal.Decimal('6')])
    assert result == [5, 6]
    result = rick._attr2obj((decimal.Decimal('7'), decimal.Decimal('8')))
    assert result == (7, 8)
    rick.ValueMap = {'key': 'val'}
    result = rick._attr2obj(rick.ValueMap)
    assert result == {'key': 'val'}
    result = rick._attr2obj({'key2': 'val2'})
    assert result == {'key2': 'val2'}
    result = rick._attr2obj(datetime.datetime(2020, 5, 17))
    assert result == '2020-05-17T00:00:00'
    result = rick._attr2obj(decimal.Decimal('9'))
    assert result == 9


def test_get_save_args():
    save_args = rick._get_save_args(item=False, attributes=False, null_check=False)
    assert save_args == {'hash_key': 'person#1234', 'range_key': 'person#ricksanchez'}
    save_args = rick._get_save_args(item=True, attributes=True, null_check=True)
    assert save_args == {'hash_key': 'person#1234', 'range_key': 'person#ricksanchez', 'attributes': {'Age': 70, 'FirstName': 'Rick', 'LastName': 'Sanchez', 'PK': 'person#1234', 'SK': 'person#ricksanchez', 'Type': 'person', 'ValueList': [1,'2'],'ValueMap': {'key': 'val'}}, 'item': {'Age': 70, 'FirstName': 'Rick', 'LastName': 'Sanchez', 'PK': 'person#1234', 'SK': 'person#ricksanchez', 'Type': 'person', 'ValueList': [1,'2'], 'ValueMap': {'key': 'val'}}}


def test_get_operation_kwargs_from_instance():
    kwargs = rick.get_operation_kwargs_from_instance()
    assert kwargs == {'TableName': 'unit-test-table'}
    kwargs = rick.get_operation_kwargs_from_instance(
        actions=[Person.FirstName.set('Kalvin'), Person.LastName.set('Tronkenmueller')],
        condition=Person.FirstName.eq("Rick"),
        return_values='UPDATED_NEW',
        return_values_on_condition_failure='ALL_OLD',
        serialize=True,
        add_identifier_map=True,
        item=True
    )
    assert kwargs == {'TableName': 'unit-test-table', 'Key': {'PK': {'S': 'person#1234'}, 'SK': {'S': 'person#ricksanchez'}}, 'ConditionExpression': '#n0 = :v0', 'ReturnValues': 'UPDATED_NEW', 'ReturnValuesOnConditionCheckFailure': 'ALL_OLD', 'UpdateExpression': 'SET #n0 = :1, #1 = :2', 'ExpressionAttributeNames': {'#n0': 'FirstName', '#1': 'LastName'}, 'ExpressionAttributeValues': {':v0': {'S': 'Rick'}, ':1': {'S': 'Kalvin'}, ':2': {'S': 'Tronkenmueller'}}}


def test_get_operation_kwargs_from_class():
    kwargs = Person.get_operation_kwargs_from_class()

    assert kwargs == {'TableName': 'unit-test-table'}
    kwargs = Person.get_operation_kwargs_from_class(
        condition=Person.FirstName.eq("Rick"),
        filter_condition=Person.LastName.eq("Sanchez"),
        attributes_to_get=['FirstName', 'LastName'],
        consistent_read=True,
        select='ALL_ATTRIBUTES',
        scan_index_forward=True,
        limit=200,
    )
    assert kwargs == {'TableName': 'unit-test-table', 'ConditionExpression': '#n0 = :v0', 'FilterExpression': Person.LastName.eq("Sanchez"), 'ConsistentRead': True, 'ScanIndexForward': True, 'Limit': 200, 'ProjectionExpression': '#n0, #1', 'Select': 'ALL_ATTRIBUTES', 'ExpressionAttributeNames': {'#n0': 'FirstName', '#1': 'LastName'}, 'ExpressionAttributeValues': {':v0': 'Rick'}}


def test_get_identifier_map():
    kwargs = Person.get_identifier_map(hash_key='person#1234')
    assert kwargs == {'Key': {'PK': {'S': 'person#1234'}}}
    kwargs = Person.get_identifier_map(hash_key='person#1234', range_key='person#ricksanchez', serialize=False)
    assert kwargs == {'Key': {'PK': 'person#1234', 'SK': 'person#ricksanchez'}}


def test_get_return_values_on_condition_failure_map():
    return_values_on_condition_failure = Person.get_return_values_on_condition_failure_map('ALL_NEW')
    assert return_values_on_condition_failure == {'ReturnValuesOnConditionCheckFailure': 'ALL_NEW'}
    with pytest.raises(ValueError):
        Person.get_return_values_on_condition_failure_map('ALL_JOST')


def test_get_item_collection_map():
    item_collection_map = Person.get_item_collection_map('SIZE')
    assert item_collection_map == {'ReturnItemCollectionMetrics': 'SIZE'}
    with pytest.raises(ValueError):
        Person.get_item_collection_map('JOST')


def test_get_get_return_values_map():
    return_values_map = Person.get_return_values_map('ALL_NEW')
    assert return_values_map == {'ReturnValues': 'ALL_NEW'}
    with pytest.raises(ValueError):
        Person.get_item_collection_map('ALL_JOST')


def test_get_consumed_capacity_map():
    capacity_map = Person.get_consumed_capacity_map('TOTAL')
    assert capacity_map == {'ReturnConsumedCapacity': 'TOTAL'}
    with pytest.raises(ValueError):
        Person.get_consumed_capacity_map('JOST')