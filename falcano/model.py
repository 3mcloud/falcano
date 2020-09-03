'''
Handle connections and queries to the Team Model
'''
import time
import os
from decimal import Decimal
from datetime import datetime
from inspect import getmembers
import logging
from typing import (
    Any, Optional, Dict, Tuple, Type,
    Mapping, List, cast
)
import stringcase
import boto3
import botocore
from falcano.settings import get_settings_value
from falcano.indexes import Index, GlobalSecondaryIndex
from falcano.paginator import Results
from falcano.exceptions import TableDoesNotExist, DoesNotExist
from falcano.attributes import (
    Attribute,
    AttributeContainerMeta,
    MapAttribute,
    TTLAttribute,
    UTCDateTimeAttribute
)
from falcano.expressions.update import Update

from falcano.constants import (
    BATCH_WRITE_PAGE_LIMIT, DELETE, PUT, ATTR_TYPE_MAP, ATTR_NAME, ATTR_TYPE, RANGE, HASH, ITEMS,
    BILLING_MODE, GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, READ_CAPACITY_UNITS, ITEM,
    WRITE_CAPACITY_UNITS, PROJECTION, INDEX_NAME, PROJECTION_TYPE, PAY_PER_REQUEST_BILLING_MODE,
    ATTRIBUTES, META_CLASS_NAME, REGION, HOST, ATTR_DEFINITIONS, KEY_SCHEMA, KEY_TYPE, TABLE_NAME,
    PROVISIONED_THROUGHPUT, NON_KEY_ATTRIBUTES, RANGE_KEY, CONDITION_EXPRESSION, UPDATE_EXPRESSION,
    EXPRESSION_ATTRIBUTE_NAMES, EXPRESSION_ATTRIBUTE_VALUES, RETURN_VALUES, ALL_NEW, KEY,
    RESPONSES, BATCH_GET_PAGE_LIMIT, UNPROCESSED_KEYS, KEYS
)

logger = logging.getLogger('entity-base')  # pylint: disable=invalid-name

_KeyType = Any


ATTR_MAP = {
    REGION: 'region',
    HOST: 'host',
    'connect_timeout_seconds': 'connect_timeout_seconds',
    'read_timeout_seconds': 'read_timeout_seconds',
    'base_backoff_ms': 'base_backoff_ms',
    'max_retry_attempts': 'max_retry_attempts',
    'max_pool_connections': 'max_pool_connections',
    'extra_headers': 'extra_headers',
    'aws_access_key_id': None,
    'aws_secret_access_key': None,
    'aws_session_token': None,
}


class MetaModel(AttributeContainerMeta):
    '''
    Model meta class
    This class is just here so that index queries have nice syntax.
    Model.index.query()
    '''
    table_name: str
    read_capacity_units: Optional[int]
    write_capacity_units: Optional[int]
    region: Optional[str]
    host: Optional[str]
    connect_timeout_seconds: int
    read_timeout_seconds: int
    base_backoff_ms: int
    max_retry_attempts: int
    max_pool_connections: int
    extra_headers: Mapping[str, str]
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    aws_session_token: Optional[str]
    billing_mode: Optional[str]
    stream_view_type: Optional[str]

    def __init__(cls, name: str, bases: Any, attrs: Dict[str, Any]) -> None:
        super().__init__(name, bases, attrs)
        m_cls = cast(Type['Model'], cls)

        members = getmembers(m_cls)
        for attr_name, attr_obj in members:
            if attr_name == META_CLASS_NAME:
                for attr in ATTR_MAP:
                    if not hasattr(attr_obj, attr):
                        if ATTR_MAP[attr] is None:
                            setattr(attr_obj, attr, None)
                            continue
                        setattr(attr_obj, attr, get_settings_value(ATTR_MAP[attr]))
            elif isinstance(attr_obj, Index):
                attr_obj.Meta.model = m_cls
                if not hasattr(attr_obj.Meta, "index_name"):
                    attr_obj.Meta.index_name = attr_name
            elif isinstance(attr_obj, Attribute):
                if attr_obj.attr_name is None:
                    attr_obj.attr_name = attr_name

        ttl_attr_names = [name for name,
                          attr_obj in attrs.items() if isinstance(attr_obj, TTLAttribute)]
        if len(ttl_attr_names) > 1:
            raise ValueError("The model has more than one TTL attribute: {}".format(
                ", ".join(ttl_attr_names)))

        cls.store_parent_classes(m_cls)  # pylint: disable=no-value-for-parameter

    def store_parent_classes(self, cls):  # pylint: disable=no-self-use, bad-mcs-method-argument
        '''
        Add this as a child to all parent class _models, recursively if it has a model_type
        '''
        # Skip if there is no type
        try:
            model_type = getattr(cls, cls.Meta.model_type)
        except AttributeError:
            return
        added = set()
        queue = {cls}

        while len(queue) > 0:
            cur_cls = queue.pop()
            cur_cls._models[model_type.default] = cls
            # add all the parent's parents if they're not already in added
            for parent in cur_cls.__bases__:
                if parent not in added and hasattr(parent, 'Meta'):
                    queue.add(parent)
                    added.add(parent)


class Model(metaclass=MetaModel):
    '''
    Model class
    '''
    _connection = None
    _resource = None
    _attributes = {}
    _dynamo_to_python_attrs = {}
    _indexes = None
    attribute_values = {}
    _models = {}

    def __init__(self,
                 hash_key: Optional[_KeyType] = None,
                 range_key: Optional[_KeyType] = None,
                 _user_instantiated: bool = True,
                 **attributes: Any,):
        if hash_key is not None:
            attributes[self.get_hash_key().attr_name] = hash_key
        if range_key is not None:
            attributes[self.get_range_key().attr_name] = range_key
        self.attribute_values: Dict[str, Any] = {}
        self.initialize_attributes(_user_instantiated, **attributes)
        self.convert_decimal = True

    class Meta():
        '''
        Meta class for the Model
        '''
        max_pool_connections = 20
        connect_timeout_seconds = 10
        read_timeout_seconds = 5
        billing_mode = 'PAY_PER_REQUEST'
        table_name = os.getenv('DYNAMODB_TABLE')
        host = os.getenv('ENDPOINT_URL', None)
        _table = None
        separator = '#'
        model_type = 'Type'

        @property
        def table(self):
            '''
            Table property getter
            '''
            return self._table

        @table.setter
        def table(self, name):
            '''
            Setter for the table name
            '''
            dynamodb = boto3.client('dynamodb', endpoint_url=self.host)
            self._table = dynamodb.Table(name)

    def _set_defaults(self, _user_instantiated: bool = True) -> None:
        '''
        Sets and fields that provide a default value
        '''
        for name, attr in self.get_attributes():
            if _user_instantiated and attr.default_for_new is not None:
                default = attr.default_for_new
            else:
                default = attr.default
            if callable(default):
                value = default()
            else:
                value = default
            if value is not None:
                setattr(self, name, value)

    def initialize_attributes(self, _user_instantiated: bool, **attributes: Attribute):
        '''
        Initialize the attributes of the Model
        '''
        self._attributes = self.get_attributes()
        self._set_defaults(_user_instantiated=_user_instantiated)
        keys = []
        for attr_name, attribute in self._attributes:
            # if attribute.is_hash_key:
            #     self._hash_keyname = attr_name
            # if attribute.is_range_key:
            #     self._range_keyname = attr_name
            self._dynamo_to_python_attrs[attribute.attr_name] = attr_name
            keys.append(attr_name)

        for attr_name, attr_value in attributes.items():
            if attr_name not in keys:
                raise ValueError(f'Attribute {attr_name} specified does not exist')
            setattr(self, attr_name, attr_value)

    @classmethod
    def get_attributes(cls) -> List[Tuple[str, Attribute]]:
        '''
        Get attributes of the calling class for dynamodb
        '''
        members = getmembers(cls, lambda o: isinstance(o, Attribute))
        # for name, member in members:
        #     if not member.attr_name:
        #         member.attr_name = name
        return members

    @classmethod
    def get_attribute(cls, name) -> Attribute:
        '''
        Get a single attribute by the name
        '''
        for tup in cls.get_attributes():
            if tup[0] == name:
                return tup[1]
        return None

    @classmethod
    def get_hash_key(cls) -> Attribute:
        '''
        Get the hash key
        '''
        members = cls.get_attributes()
        for _, member in members:
            if member.is_hash_key:
                return member
        return None

    @classmethod
    def get_range_key(cls) -> Attribute:
        '''
        Get the range key
        '''
        members = cls.get_attributes()
        for _, member in members:
            if member.is_range_key:
                return member
        return None

    @classmethod
    def connection(cls):
        '''
        Returns a (cached) connection
        '''
        if cls._connection is None:
            cls._connection = boto3.client(
                'dynamodb',
                endpoint_url=cls.Meta.host,
                config=botocore.config.Config(
                    max_pool_connections=cls.Meta.max_pool_connections,
                    connect_timeout=cls.Meta.connect_timeout_seconds,
                    read_timeout=cls.Meta.read_timeout_seconds,
                )
            )
        return cls._connection

    @classmethod
    def resource(cls):
        '''
        Returns a (cached) connection
        '''

        if cls._resource is None:
            # attr = getattr(cls, cls.Meta.model_type)
            # cls._models[attr.default] = cls
            cls._resource = boto3.resource(
                'dynamodb',
                endpoint_url=cls.Meta.host,
                config=botocore.config.Config(
                    max_pool_connections=cls.Meta.max_pool_connections,
                    connect_timeout=cls.Meta.connect_timeout_seconds,
                    read_timeout=cls.Meta.read_timeout_seconds,
                )
            )
        return cls._resource

    @classmethod
    def _get_indexes(cls) -> Dict[str, Dict]:
        '''
        Returns a list of the secondary indexes
        '''
        if cls._indexes is None:
            cls._indexes = {
                ATTR_DEFINITIONS: []
            }
            cls._index_classes = {}
            for _, index in getmembers(cls, lambda o: isinstance(o, Index)):
                cls._index_classes[index.Meta.index_name] = index
                schema = index._get_schema()
                idx = {
                    INDEX_NAME: index.Meta.index_name,
                    KEY_SCHEMA: schema.get(KEY_SCHEMA),
                    PROJECTION: {
                        PROJECTION_TYPE: index.Meta.projection.projection_type,
                    },

                }
                if isinstance(index, GlobalSecondaryIndex):
                    if getattr(cls.Meta, 'billing_mode', None) != PAY_PER_REQUEST_BILLING_MODE:
                        idx[PROVISIONED_THROUGHPUT] = {
                            READ_CAPACITY_UNITS: index.Meta.read_capacity_units,
                            WRITE_CAPACITY_UNITS: index.Meta.write_capacity_units
                        }
                cls._indexes.get(ATTR_DEFINITIONS).extend(
                    schema.get(ATTR_DEFINITIONS))
                if index.Meta.projection.non_key_attributes:
                    idx[PROJECTION][NON_KEY_ATTRIBUTES] = index.Meta.projection.non_key_attributes
                if isinstance(index, GlobalSecondaryIndex):
                    cls._indexes.setdefault(GLOBAL_SECONDARY_INDEXES, [])
                    cls._indexes.get(GLOBAL_SECONDARY_INDEXES).append(idx)
                else:
                    cls._indexes.setdefault(LOCAL_SECONDARY_INDEXES, [])
                    cls._indexes.get(LOCAL_SECONDARY_INDEXES).append(idx)
        return cls._indexes

    @classmethod
    def _get_schema(cls):
        '''
        Returns the schema for this table
        '''
        schema: Dict[str, List] = {
            ATTR_DEFINITIONS: [],
            KEY_SCHEMA: [],
            TABLE_NAME: cls.Meta.table_name,
            BILLING_MODE: cls.Meta.billing_mode
        }
        for attr_name, attr_cls in cls.get_attributes():
            if attr_cls.is_hash_key or attr_cls.is_range_key:
                schema[ATTR_DEFINITIONS].append({
                    ATTR_NAME: attr_name,
                    ATTR_TYPE: ATTR_TYPE_MAP[attr_cls.attr_type]
                })
            if attr_cls.is_hash_key:
                # Ensure the hash key is the first itme in the list
                schema[KEY_SCHEMA].insert(0, {
                    KEY_TYPE: HASH,
                    ATTR_NAME: attr_cls.attr_name
                })
            elif attr_cls.is_range_key:
                schema[KEY_SCHEMA].append({
                    KEY_TYPE: RANGE,
                    ATTR_NAME: attr_cls.attr_name
                })
        return schema

    @classmethod
    def create_table(cls, wait: bool = False):
        '''
        Create a table in DynamoDB
        '''
        logger.debug('CREATE THE TABLE')

        schema = cls._get_schema()

        index_data = cls._get_indexes()
        if index_data.get(GLOBAL_SECONDARY_INDEXES) is not None:
            schema[GLOBAL_SECONDARY_INDEXES] = index_data.get(GLOBAL_SECONDARY_INDEXES)
        if index_data.get(LOCAL_SECONDARY_INDEXES) is not None:
            schema[LOCAL_SECONDARY_INDEXES] = index_data.get(LOCAL_SECONDARY_INDEXES)
        index_attrs = index_data.get(ATTR_DEFINITIONS)
        attr_keys = [attr[ATTR_NAME] for attr in schema[ATTR_DEFINITIONS]]
        for attr in index_attrs:
            attr_name = attr[ATTR_NAME]
            if attr_name not in attr_keys:
                schema[ATTR_DEFINITIONS].append(attr)
                attr_keys.append(attr_name)

        dynamodb = cls.connection()
        try:
            dynamodb.create_table(**schema)
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'ResourceInUseException':
                pass
            else:
                raise error

        if wait:
            while True:
                try:
                    response = cls.connection().describe_table(TableName=cls.Meta.table_name)
                    status = response['Table']['TableStatus']
                    if status in ('CREATING', 'UPDATING', 'DELETING', 'ARCHIVING'):
                        time.sleep(2)
                    else:
                        break
                except botocore.exceptions.ClientError as error:
                    if error.response['Error']['Code'] == 'ResourceNotFoundException':
                        raise KeyError('There is no table %s, on Zuul' % cls.Meta.table_name)
                    raise error

    @classmethod
    def scan(
        cls,
        **kwargs
    ):
        '''
        Provides a high level scan API
        '''
        table = cls.resource().Table(cls.Meta.table_name)
        response = table.scan(**kwargs)
        return Results(
            Model,
            response
        )

    @classmethod
    def get(
        cls,
        hash_key,
        range_key=None,
        **kwargs
    ):
        '''
        Provides a high level get_item API
        '''
        table = cls.resource().Table(cls.Meta.table_name)
        kwargs['Key'] = {cls.get_hash_key().attr_name: hash_key}
        if range_key is not None:
            kwargs['Key'][cls.get_range_key().attr_name] = range_key
        res = table.get_item(**kwargs)
        if res and (data := res.get(ITEM)):
            return Model._models[data[cls.Meta.model_type]](**data)
        raise DoesNotExist()

    @classmethod
    def batch_get(
        cls,
        items,
        **kwargs
    ):
        '''
        Provides a high level batch_get_item API

        :param items: contains a list of dicts of primary/sort key-value pairs
        '''
        hash_key_attribute = cls.get_hash_key()
        range_key_attribute = cls.get_range_key()
        keys_to_get: List[Any] = []
        records = []
        while items:
            item = items.pop()
            if range_key_attribute:
                hash_key, range_key = cls._serialize_keys(item[0], item[1])  # type: ignore
                keys_to_get.append({
                    hash_key_attribute.attr_name: hash_key,
                    range_key_attribute.attr_name: range_key
                })
            else:
                hash_key = cls._serialize_keys(item)[0]
                keys_to_get.append({
                    hash_key_attribute.attr_name: hash_key
                })

            if len(keys_to_get) == BATCH_GET_PAGE_LIMIT or not items:
                while keys_to_get:
                    page, unprocessed_keys = cls._batch_get_page(
                        keys_to_get,
                        **kwargs
                    )
                    records += page
                    if unprocessed_keys:
                        keys_to_get = unprocessed_keys
                    else:
                        keys_to_get = []

        return Results(
            Model,
            {'Items': records}
        )

    @classmethod
    def _batch_get_page(cls, keys_to_get, **kwargs):
        '''
        Returns a single page from BatchGetItem
        Also returns any unprocessed items

        :param keys_to_get: A list of keys
        '''
        logger.debug('Fetching a BatchGetItem page')

        resource = cls.resource()
        table_name = cls.Meta.table_name
        kwargs['Keys'] = keys_to_get
        kwargs = {'RequestItems': {cls.Meta.table_name: kwargs}}

        data = resource.batch_get_item(**kwargs)
        item_data = data.get(RESPONSES).get(cls.Meta.table_name)  # type: ignore
        unprocessed_items = data.get(UNPROCESSED_KEYS).get(
            cls.Meta.table_name, {}).get(KEYS, None)  # type: ignore
        return item_data, unprocessed_items

    @classmethod
    def exists(cls) -> bool:
        '''
        Returns True if this table exists, False otherwise
        '''
        try:
            cls.connection().describe_table(TableName=cls.Meta.table_name)
            return True
        except cls.connection().exceptions.ResourceNotFoundException:
            return False

    @classmethod
    def query(cls,
              hash_key,
              range_key_condition=None,
              filter_condition=None,
              consistent_read=False,
              index_name=None,
              scan_index_forward=None,
              limit=None,
              last_evaluated_key=None,
              attributes_to_get=None,
              page_size=None):
        '''
        Provides a high level query API

        :param hash_key: The hash key to query
        :param range_key_condition: Condition for range key
        :param filter_condition: Condition used to restrict the query results
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param limit: Used to limit the number of results returned
        :param scan_index_forward: If set, then used to specify the same parameter to the DynamoDB API.
            Controls descending or ascending results
        :param last_evaluated_key: If set, provides the starting point for query.
        :param page_size: Page size of the query to DynamoDB
        '''

        if index_name:
            hash_attr = index_name._hash_key_attribute()
        else:
            hash_attr = cls.get_hash_key()

        query = {
            'TableName': cls.Meta.table_name,
            'KeyConditionExpression': hash_attr.eq(hash_key),
        }
        if range_key_condition:
            query['KeyConditionExpression'] = query['KeyConditionExpression'] & range_key_condition
        if filter_condition:
            query['FilterExpression'] = filter_condition
        if consistent_read:
            query['ConsistentRead'] = consistent_read
        if index_name:
            query['IndexName'] = index_name.Meta.index_name
            query['Select'] = 'ALL_PROJECTED_ATTRIBUTES'
        if scan_index_forward:
            query['ScanIndexForward'] = scan_index_forward
        if limit:
            query['Limit'] = limit
        if last_evaluated_key:
            query['ExclusiveStartKey'] = last_evaluated_key
        if attributes_to_get:
            # Legacy version
            # query['Select'] = 'SPECIFIC_ATTRIBUTES'
            # query['AttributesToGet'] = attributes_to_get

            # Use #A1, #A2, ... to avoid reserved word conflicts
            names = {f'#A{i}': attr for i, attr in enumerate(attributes_to_get)}
            query['Select'] = 'SPECIFIC_ATTRIBUTES'
            query['ProjectionExpression'] = ', '.join(list(names.keys()))
            query['ExpressionAttributeNames'] = names
        if page_size:
            query['Limit'] = page_size
        logger.debug('--query %s', query)
        table = cls.resource().Table(cls.Meta.table_name)
        response = table.query(**query)
        return Results(Model, response)

    def _serialize(self, attr_map=False, null_check=True) -> Dict[str, Any]:
        '''
        Serializes all model attributes for use with DynamoDB
        :param attr_map: If True, then attributes are returned
        :param null_check: If True, then attributes are checked for null
        '''
        attrs: Dict[str, Dict] = {ATTRIBUTES: {}}
        for name, attr in self.get_attributes():
            value = getattr(self, name)
            if isinstance(value, MapAttribute):
                if not value.validate():
                    raise ValueError("Attribute '{}' is not correctly typed".format(attr.attr_name))

            if value is None:
                continue
            serialized = attr.serialize(value)
            if serialized is None and not attr.null and null_check:
                raise ValueError(f"Attribute '{attr.attr_name}' cannot be None")

            if attr_map:
                logger.warning('Unsupported argument: attr_map')

            attrs[ATTRIBUTES][attr.attr_name] = serialized

        return attrs

    def _get_keys(self):
        '''
        Returns the proper arguments for deleting
        '''
        hash_key = self.get_hash_key()
        range_key = self.get_range_key()
        attrs = {
            hash_key.attr_name: getattr(self, hash_key.attr_name),
        }
        if range_key is not None:
            attrs[range_key.attr_name] = getattr(self, range_key.attr_name)
        return attrs

    @classmethod
    def _serialize_value(cls, attr, value):  # , null_check=True):
        '''
        Serializes a value for use with DynamoDB
        :param attr: an instance of `Attribute` for serialization
        :param value: a value to be serialized
        # :param null_check: If True, then attributes are checked for null
        '''
        if value is None:
            serialized = None
        else:
            serialized = attr.serialize(value)
        return serialized

    @classmethod
    def _serialize_keys(cls, hash_key, range_key=None) -> Tuple[_KeyType, _KeyType]:
        '''
        Serializes the hash and range keys
        :param hash_key: The hash key value
        :param range_key: The range key value
        '''
        hash_key = cls.get_hash_key().serialize(hash_key)
        if range_key is not None:
            range_key = cls.get_range_key().serialize(range_key)
        return hash_key, range_key

    @classmethod
    def from_raw_data(cls, data):
        '''
        Returns an instance of this class
        from the raw data

        :param data: A serialized DynamoDB object
        '''
        if data is None:
            raise ValueError('Received no data to construct object')

        attributes = {}
        for name, value in data.items():
            attr_name = cls._dynamo_to_python_attr(name)
            attr = cls.get_attribute(attr_name)
            if attr:
                attributes[attr_name] = attr.deserialize(attr.get_value(value))
        return cls(_user_instantiated=False, **attributes)

    @classmethod
    def _dynamo_to_python_attr(cls, dynamo_key: str) -> Optional[str]:
        '''
        Convert a DynamoDB attribute name to the internal Python name.

        This covers cases where an attribute name has been overridden via 'attr_name'.
        '''
        return cls._dynamo_to_python_attrs.get(dynamo_key, dynamo_key)  # type: ignore

    @classmethod
    def batch_write(cls, auto_commit: bool = True):
        '''
        Returns a BatchWrite context manager for a batch operation.

        :param auto_commit: If true, the context manager will commit writes incrementally
                            as items are written to as necessary to honor item count limits
                            in the DynamoDB API (see BatchWrite). Regardless of the value
                            passed here, changes automatically commit on context exit
                            (whether successful or not).
        '''
        return BatchWrite(cls, auto_commit=auto_commit)

    def delete(self, condition=None) -> Any:
        kwargs = {}
        table = self.resource().Table(self.Meta.table_name)
        if condition:
            kwargs['ConditionExpression'] = condition
        kwargs['Key'] = self._get_keys()
        return table.delete_item(**kwargs)

    def update(self, actions, condition=None):
        '''
        Updates an item using the UpdateItem operation.
        '''
        if not isinstance(actions, list) or len(actions) == 0:
            raise TypeError('the value of `actions` is expected to be a non-empty list')

        kwargs = {
            RETURN_VALUES: ALL_NEW,
        }
        if condition is not None:
            kwargs[CONDITION_EXPRESSION] = condition

        name_placeholders: Dict[str, str] = {}
        expression_attribute_values: Dict[str, Any] = {}
        if actions is not None:
            update_expression = Update(*actions)
            kwargs[UPDATE_EXPRESSION] = update_expression.serialize(
                name_placeholders,
                expression_attribute_values
            )
        if name_placeholders:
            kwargs[EXPRESSION_ATTRIBUTE_NAMES] = {v: k for k, v in name_placeholders.items()}
        if expression_attribute_values:
            kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values

        # Get the key and put it in kwargs
        kwargs[KEY] = self._get_keys()
        table = self.resource().Table(self.Meta.table_name)
        res = table.update_item(**kwargs)['Attributes']
        return Model._models[res[self.Meta.model_type]](**res)

    def save(self, condition=None) -> Dict[str, Any]:
        ''' Save a falcano model into dynamodb '''
        kwargs = {'Item': self._serialize()[ATTRIBUTES]}
        if condition:
            kwargs['ConditionExpression'] = condition
        table = self.resource().Table(self.Meta.table_name)
        data = table.put_item(**kwargs)
        return data

    def to_dict(self, primary_key: str = 'PK', convert_decimal: bool = True):
        '''Convert a pynamo model into a dictionary for JSON serialization'''
        # temporary override of converting decimal to int/float
        self.convert_decimal = convert_decimal
        ret_dict = {}
        for name, attr in self.attribute_values.items():
            ret_dict[name] = self._attr2obj(attr)
        ret_dict['ID'] = ret_dict[primary_key].split(self.Meta.separator)[-1]
        return ret_dict

    def _attr2obj(self, attr):
        '''Turn a pynamo Attribute into a dict'''
        if isinstance(attr, list):
            _list = []
            for item in attr:
                _list.append(self._attr2obj(item))
            return _list
        if isinstance(attr, set):
            _set = set()
            for item in attr:
                _set.add(self._attr2obj(item))
            return _set
        if isinstance(attr, MapAttribute):
            # Convert the map attribute with boto3 typedeserializer
            _dict = {}
            deserializer = boto3.dynamodb.types.TypeDeserializer()
            for key, value in attr.attribute_values.items():
                _dict[key] = deserializer.deserialize(value)
            # Traverse the new dict and convert to simple types
            # for easier serialization later
            return self._attr2obj(_dict)
        if isinstance(attr, dict):
            # Handle dictionary types
            _dict = {}
            for key, value in attr.items():
                _dict[key] = self._attr2obj(value)
            return _dict
        if isinstance(attr, datetime):
            return attr.isoformat()
        if isinstance(attr, Decimal):
            if self.convert_decimal:
                # Attempt to convert a more accurate decimal to float or int
                return int(attr) if attr % 1 == 0 else float(attr)
            return attr

        return attr


class ModelContextManager():
    '''
    A class for managing batch operations
    '''

    def __init__(self, model, auto_commit: bool = True):
        self.model = model
        self.auto_commit = auto_commit
        self.max_operations = BATCH_WRITE_PAGE_LIMIT
        self.pending_operations: List[Dict[str, Any]] = []
        self.failed_operations: List[Any] = []

    def __enter__(self):
        return self


class BatchWrite(ModelContextManager):
    '''
    A class for batch writes
    '''

    def save(self, put_item) -> None:
        '''
        This adds `put_item` to the list of pending operations to be performed.

        If the list currently contains 25 items, which is the DynamoDB imposed
        limit on a BatchWriteItem call, one of two things will happen. If auto_commit
        is True, a BatchWriteItem operation will be sent with the already pending
        writes after which put_item is appended to the (now empty) list. If auto_commit
        is False, ValueError is raised to indicate additional items cannot be accepted
        due to the DynamoDB imposed limit.

        :param put_item: Should be an instance of a `Model` to be written
        '''
        if len(self.pending_operations) == self.max_operations:
            if not self.auto_commit:
                raise ValueError("DynamoDB allows a maximum of 25 batch operations")
            self.commit()
        self.pending_operations.append({"Action": PUT, "Item": put_item})

    def delete(self, del_item) -> None:
        '''
        This adds `del_item` to the list of pending operations to be performed.

        If the list currently contains 25 items, which is the DynamoDB imposed
        limit on a BatchWriteItem call, one of two things will happen. If auto_commit
        is True, a BatchWriteItem operation will be sent with the already pending
        operations after which put_item is appended to the (now empty) list. If auto_commit
        is False, ValueError is raised to indicate additional items cannot be accepted
        due to the DynamoDB imposed limit.

        :param del_item: Should be an instance of a `Model` to be deleted
        '''
        if len(self.pending_operations) == self.max_operations:
            if not self.auto_commit:
                raise ValueError("DynamoDB allows a maximum of 25 batch operations")
            self.commit()
        self.pending_operations.append({"Action": DELETE, "Item": del_item})

    def __exit__(self, exc_type, exc_val, exc_tb):
        '''
        This ensures that all pending operations are committed when
        the context is exited
        '''
        return self.commit()

    def commit(self) -> None:
        '''
        Writes all of the changes that are pending
        '''
        logger.debug('%s committing batch operation', self.model)
        put_items = []
        delete_items = []
        for item in self.pending_operations:
            if item['Action'] == PUT:
                put_items.append(item['Item']._serialize()[ATTRIBUTES])
            elif item['Action'] == DELETE:
                delete_items.append(item['Item']._get_keys())
        self.pending_operations = []
        if not delete_items and not put_items:
            return

        table = self.model.resource().Table(self.model.Meta.table_name)
        with table.batch_writer() as batch:
            for item in put_items:
                batch.put_item(Item=item)
            for item in delete_items:
                batch.delete_item(Key=item)
