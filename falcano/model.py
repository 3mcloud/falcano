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
import boto3
import boto3.dynamodb.types as dynamo_types
from boto3.dynamodb.conditions import ConditionExpressionBuilder
import botocore
import stringcase
from falcano.settings import get_settings_value
from falcano.indexes import Index, GlobalSecondaryIndex
from falcano.paginator import Results
from falcano.exceptions import DoesNotExist, InvalidStateError
from falcano.attributes import (
    Attribute,
    AttributeContainerMeta,
    MapAttribute,
    TTLAttribute,
)
from falcano.expressions.update import Update
from falcano.expressions.projection import create_projection_expression

from falcano.constants import (
    BATCH_WRITE_PAGE_LIMIT, DELETE, PUT, ATTR_TYPE_MAP, ATTR_NAME, ATTR_TYPE, RANGE, HASH,
    BILLING_MODE, GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, READ_CAPACITY_UNITS, ITEM,
    WRITE_CAPACITY_UNITS, PROJECTION, INDEX_NAME, PROJECTION_TYPE, PAY_PER_REQUEST_BILLING_MODE,
    ATTRIBUTES, META_CLASS_NAME, REGION, HOST, ATTR_DEFINITIONS, KEY_SCHEMA, KEY_TYPE, TABLE_NAME,
    PROVISIONED_THROUGHPUT, NON_KEY_ATTRIBUTES, RANGE_KEY, HASH_KEY, CONDITION_EXPRESSION, REQUEST_ITEMS,
    UPDATE_EXPRESSION, EXPRESSION_ATTRIBUTE_NAMES, EXPRESSION_ATTRIBUTE_VALUES, RETURN_VALUES,
    ALL_NEW, KEY, RESPONSES, BATCH_GET_PAGE_LIMIT, UNPROCESSED_KEYS, KEYS, TRANSACT_CONDITION_CHECK,
    TRANSACT_DELETE, TRANSACT_PUT, TRANSACT_UPDATE, RETURN_VALUES_VALUES, RETURN_VALUES_ON_CONDITION_FAILURE, RETURN_VALUES_ON_CONDITION_FAILURE_VALUES,
    TRANSACT_GET, TRANSACT_ITEMS, RETURN_CONSUMED_CAPACITY, PROJECTION_EXPRESSION, CONSISTENT_READ, KEY_CONDITION_EXPRESSION,
    FILTER_EXPRESSION, SELECT, ALL_PROJECTED_ATTRIBUTES, SCAN_INDEX_FORWARD, LIMIT, EXCLUSIVE_START_KEY,
    SPECIFIC_ATTRIBUTES, RETURN_ITEM_COLL_METRICS_VALUES, RETURN_ITEM_COLL_METRICS, RETURN_CONSUMED_CAPACITY_VALUES,
    TABLE_KEY, TABLE_STATUS, ITEMS, ACTION
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

    def __delattr__(self, name):
        '''Remove an attribute from a model.

        Useful for gets with projection expressions
        '''
        del self.__dict__['attribute_values'][name]
        return super().__delattr__(name)

    class Meta():
        '''
        Meta class for the Model
        '''
        max_pool_connections = 20
        connect_timeout_seconds = 10
        read_timeout_seconds = 5
        billing_mode = PAY_PER_REQUEST_BILLING_MODE
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
                    if getattr(cls.Meta, stringcase.snakecase(BILLING_MODE), None) != PAY_PER_REQUEST_BILLING_MODE:
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
                    status = response[TABLE_KEY][TABLE_STATUS]
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
        kwargs = cls.get_operation_kwargs_from_class(**kwargs)
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
        kwargs['add_identifier_map'] = True
        kwargs['serialize'] = False
        kwargs[stringcase.snakecase(HASH_KEY)] = hash_key
        if range_key is not None:
            kwargs['range_key'] = range_key
        kwargs = cls.get_operation_kwargs_from_class(**kwargs)
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
            {ITEMS: records}
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
        kwargs[KEYS] = keys_to_get
        kwargs = {REQUEST_ITEMS: {cls.Meta.table_name: kwargs}}

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
        select=None
        if index_name:
            hash_attr = index_name._hash_key_attribute()
            select=ALL_PROJECTED_ATTRIBUTES
        else:
            hash_attr = cls.get_hash_key()

        if attributes_to_get:
            select = SPECIFIC_ATTRIBUTES

        kwargs: Dict[str, Any] = dict(
            hash_key=hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            consistent_read=consistent_read,
            index_name=index_name,
            scan_index_forward=scan_index_forward,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            page_size=page_size,
            key_condition_expression=hash_attr.eq(hash_key),
            select=select
        )

        query = cls.get_operation_kwargs_from_class(**kwargs)
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

            if attr.is_hash_key:
                attrs[HASH] = serialized
            if attr.is_range_key:
                attrs[RANGE] = serialized

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
    def transact_write(cls):
        '''
        Returns a TransactWrite
        '''
        return TransactWrite(cls, connection=cls.connection())

    @classmethod
    def transact_get(cls):
        '''
        Returns a TransactGet
        '''
        return TransactGet(connection=cls.connection())

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
        kwargs = {'add_identifier_map': True, 'condition': condition}
        table = self.resource().Table(self.Meta.table_name)
        kwargs = self.get_operation_kwargs_from_instance(**kwargs)
        return table.delete_item(**kwargs)

    def update(self, actions, condition=None):
        '''
        Updates an item using the UpdateItem operation.
        '''
        if not isinstance(actions, list) or len(actions) == 0:
            raise TypeError('the value of `actions` is expected to be a non-empty list')

        kwargs = {
            stringcase.snakecase(RETURN_VALUES): ALL_NEW,
            'actions': actions,
            'add_identifier_map': True,
            'condition': condition
        }

        kwargs = self.get_operation_kwargs_from_instance(**kwargs)
        table = self.resource().Table(self.Meta.table_name)
        res = table.update_item(**kwargs)[ATTRIBUTES]
        return Model._models[res[self.Meta.model_type]](**res)

    def save(self, condition=None) -> Dict[str, Any]:
        ''' Save a falcano model into dynamodb '''
        kwargs = {'item': True, 'condition': condition}
        kwargs = self.get_operation_kwargs_from_instance(**kwargs)
        table = self.resource().Table(self.Meta.table_name)
        data = table.put_item(**kwargs)
        return data

    def to_dict(self, primary_key: str = None, convert_decimal: bool = True):
        '''Convert a pynamo model into a dictionary for JSON serialization'''
        if not primary_key:
            primary_key = self.get_hash_key().attr_name
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
            attribute_values = attr.attribute_values
            try:
                # top level map attributes will have a key
                # of 'M' that need to be deserialized
                deserializer = dynamo_types.TypeDeserializer()
                attribute_values = deserializer.deserialize(attribute_values)
            except (TypeError, AttributeError):
                pass
            for key, value in attribute_values.items():
                _dict[key] = self._attr2obj(value)
            # Traverse the new dict and convert to simple types
            # for easier serialization later
            return _dict
        if isinstance(attr, dict):
            # Handle dictionary types
            _dict = {}
            try:
                # convert types like {'baz':{'S':'qux'}} to
                # {'baz':'qux'}. Because of the recursion
                # we can get {'S': 'qux'} converted to 'qux'
                # and we just return that simple value.
                deserializer = dynamo_types.TypeDeserializer()
                attr = deserializer.deserialize(attr)
                if not isinstance(attr, dict):
                    return attr
            except (TypeError, AttributeError):
                pass

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

    def _get_save_args(self, item=False, attributes=True, null_check=True):
        '''
        Gets the proper *args, **kwargs for saving and retrieving this object

        This is used for serializing items to be saved, or for serializing just the keys.

        :param attributes: If True, then attributes are included.
        :param null_check: If True, then attributes are checked for null.
        '''
        kwargs = {}
        serialized = self._serialize(null_check=null_check)
        kwargs[stringcase.snakecase(HASH_KEY)] = serialized.get(HASH)
        if RANGE in serialized:
            kwargs[stringcase.snakecase(RANGE_KEY)] = serialized.get(RANGE)
        if attributes:
            kwargs[stringcase.snakecase(ATTRIBUTES)] = serialized.get(ATTRIBUTES)
        if item:
            kwargs[stringcase.snakecase(ITEM)] = serialized.get(ATTRIBUTES)
        return kwargs

    def get_operation_kwargs_from_instance(
        self,
        key: str = KEY,
        actions=None,
        condition=None,
        return_values=None,
        return_values_on_condition_failure=None,
        serialize=False,
        add_identifier_map=False,
        item=False
    ) -> Dict[str, Any]:
        is_update = actions is not None
        save_kwargs = self._get_save_args(
                item=item, attributes=True, null_check=not is_update)

        # version_condition = self._handle_version_attribute(
        #     serialized_attributes={} if is_delete else save_kwargs,
        #     actions=actions
        # )
        # if version_condition is not None:
        #     condition &= version_condition

        kwargs: Dict[str, Any] = dict(
            serialize=serialize,
            add_identifier_map=add_identifier_map,
            table_name=self.Meta.table_name,
            key=key,
            actions=actions,
            condition=condition,
            return_values=return_values,
            return_values_on_condition_failure=return_values_on_condition_failure
        )
        if not is_update:
            kwargs.update(save_kwargs)
        else:
            kwargs[stringcase.snakecase(HASH_KEY)] = save_kwargs[stringcase.snakecase(HASH_KEY)]
            if stringcase.snakecase(RANGE_KEY) in save_kwargs:
                kwargs[stringcase.snakecase(
                    RANGE_KEY)] = save_kwargs[stringcase.snakecase(RANGE_KEY)]
        return self.get_operation_kwargs(**kwargs)


    @classmethod
    def get_operation_kwargs_from_class(
        cls,
        hash_key=None,
        range_key=None,
        condition=None,
        attributes_to_get=None,
        key_condition_expression=None,
        range_key_condition=None,
        filter_condition=None,
        index_name=None,
        scan_index_forward=None,
        limit=None,
        last_evaluated_key=None,
        page_size=None,
        consistent_read=None,
        select=None,
        serialize=False,
        add_identifier_map=False
    ) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = dict(
            serialize=serialize,
            add_identifier_map=add_identifier_map,
            table_name=cls.Meta.table_name,
            hash_key=hash_key,
            range_key=range_key,
            condition=condition,
            attributes_to_get=attributes_to_get,
            consistent_read=consistent_read,
            key_condition_expression=key_condition_expression,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=index_name,
            scan_index_forward=scan_index_forward,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            page_size=page_size,
            select=select
        )

        return cls.get_operation_kwargs(**kwargs)

    @classmethod
    def get_identifier_map(cls, hash_key, range_key=None, key=KEY, serialize=True):
        serializer = dynamo_types.TypeSerializer()
        hash_key = serializer.serialize(hash_key) if serialize else hash_key
        kwargs = {key: {cls.get_hash_key().attr_name: hash_key}}
        if range_key:
            range_key = serializer.serialize(range_key) if serialize else range_key
            kwargs[key][cls.get_range_key().attr_name] = range_key
        return kwargs

    @classmethod
    def get_return_values_on_condition_failure_map(
        cls, return_values_on_condition_failure: str
    ) -> Dict:
        """
        Builds the return values map that is common to several operations
        """
        if return_values_on_condition_failure.upper() not in RETURN_VALUES_VALUES:
            raise ValueError("{} must be one of {}".format(
                RETURN_VALUES_ON_CONDITION_FAILURE,
                RETURN_VALUES_ON_CONDITION_FAILURE_VALUES
            ))
        return {
            RETURN_VALUES_ON_CONDITION_FAILURE: str(return_values_on_condition_failure).upper()
        }

    @classmethod
    def get_item_collection_map(cls, return_item_collection_metrics: str) -> Dict:
        """
        Builds the item collection map
        """
        if return_item_collection_metrics.upper() not in RETURN_ITEM_COLL_METRICS_VALUES:
            raise ValueError("{} must be one of {}".format(RETURN_ITEM_COLL_METRICS, RETURN_ITEM_COLL_METRICS_VALUES))
        return {
            RETURN_ITEM_COLL_METRICS: str(return_item_collection_metrics).upper()
        }

    @classmethod
    def get_return_values_map(cls, return_values: str) -> Dict:
        """
        Builds the return values map that is common to several operations
        """
        if return_values.upper() not in RETURN_VALUES_VALUES:
            raise ValueError("{} must be one of {}".format(RETURN_VALUES, RETURN_VALUES_VALUES))
        return {
            RETURN_VALUES: str(return_values).upper()
        }

    @classmethod
    def get_consumed_capacity_map(cls, return_consumed_capacity: str) -> Dict:
        """
        Builds the consumed capacity map that is common to several operations
        """
        if return_consumed_capacity.upper() not in RETURN_CONSUMED_CAPACITY_VALUES:
            raise ValueError("{} must be one of {}".format(RETURN_ITEM_COLL_METRICS, RETURN_CONSUMED_CAPACITY_VALUES))
        return {
            RETURN_CONSUMED_CAPACITY: str(return_consumed_capacity).upper()
        }

    @classmethod
    def get_operation_kwargs(
        cls,
        serialize: bool,
        add_identifier_map: bool,
        table_name: str,
        hash_key: str,
        range_key: Optional[str] = None,
        key: str = KEY,
        attributes: Optional[Any] = None,
        attributes_to_get: Optional[Any] = None,
        actions=None,
        condition=None,
        consistent_read: Optional[bool] = None,
        return_values: Optional[str] = None,
        return_consumed_capacity: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
        return_values_on_condition_failure: Optional[str] = None,
        key_condition_expression = None,
        range_key_condition = None,
        filter_condition = None,
        index_name = None,
        scan_index_forward = None,
        limit = None,
        last_evaluated_key = None,
        page_size = None,
        select=None,
        item=None
    ) -> Dict:
        operation_kwargs: Dict[str, Any] = {}
        name_placeholders: Dict[str, str] = {}
        expression_attribute_values: Dict[str, Any] = {}
        operation_kwargs[TABLE_NAME] = table_name
        if add_identifier_map:
            operation_kwargs.update(cls.get_identifier_map(hash_key, range_key, key=key, serialize=serialize))
        if item is not None:
            operation_kwargs[ITEM] = item
        if attributes and operation_kwargs.get(ITEM) is not None:  # put
            serializer = dynamo_types.TypeSerializer()
            attrs = {k: serializer.serialize(v) for k, v in attributes.items()} if serialize else attributes
            operation_kwargs[ITEM].update(attrs)
        if condition is not None:
            condition_expression, name_placeholders, expression_attribute_values = ConditionExpressionBuilder(
            ).build_expression(condition)
            operation_kwargs[CONDITION_EXPRESSION] = condition_expression
        if key_condition_expression is not None:
            operation_kwargs[KEY_CONDITION_EXPRESSION] = key_condition_expression
        if range_key_condition is not None:
            operation_kwargs[KEY_CONDITION_EXPRESSION] = operation_kwargs[KEY_CONDITION_EXPRESSION] & range_key_condition
        if filter_condition is not None:
            operation_kwargs[FILTER_EXPRESSION] = filter_condition
        if consistent_read is not None:
            operation_kwargs[CONSISTENT_READ] = consistent_read
        if index_name is not None:
            operation_kwargs[INDEX_NAME] = index_name.Meta.index_name
        if scan_index_forward is not None:
            operation_kwargs[SCAN_INDEX_FORWARD] = scan_index_forward
        if limit is not None:
            operation_kwargs[LIMIT] = limit
        if last_evaluated_key is not None:
            operation_kwargs[EXCLUSIVE_START_KEY] = last_evaluated_key
        if attributes_to_get is not None:
            name_placeholders = {v: k for k, v in name_placeholders.items()}
            projection_expression = create_projection_expression(
                attributes_to_get, name_placeholders)
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
            name_placeholders = {v: k for k, v in name_placeholders.items()}
        if select is not None:
            operation_kwargs[SELECT] = select
        if page_size is not None:
            operation_kwargs[LIMIT] = page_size
        if consistent_read is not None:
            operation_kwargs[CONSISTENT_READ] = consistent_read
        if return_values is not None:
            operation_kwargs.update(cls.get_return_values_map(return_values))
        if return_values_on_condition_failure is not None:
            operation_kwargs.update(cls.get_return_values_on_condition_failure_map(
                return_values_on_condition_failure))
        if return_consumed_capacity is not None:
            operation_kwargs.update(cls.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics is not None:
            operation_kwargs.update(cls.get_item_collection_map(return_item_collection_metrics))
        if actions is not None:
            update_expression = Update(*actions)
            # Update expressions use backwards name placeholders
            name_placeholders = {v: k for k, v in name_placeholders.items()}
            operation_kwargs[UPDATE_EXPRESSION] = update_expression.serialize(
                name_placeholders,
                expression_attribute_values
            )
            name_placeholders = {v: k for k, v in name_placeholders.items()}
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = name_placeholders
        if expression_attribute_values:
            serializer = dynamo_types.TypeSerializer()
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = {
                k: serializer.serialize(v) for k, v in expression_attribute_values.items()
            } if serialize else expression_attribute_values
        return operation_kwargs


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


class _ModelFuture():
    '''
    A placeholder object for a model that does not exist yet

    For example: when performing a TransactGet request, this is a stand-in for a model that will be returned
    when the operation is complete
    '''

    def __init__(self, model_cls) -> None:
        self._model_cls = model_cls
        self._model = None
        self._resolved = False

    def update_with_raw_data(self, data) -> None:
        if data is not None and data != {}:
            self._model = self._model_cls.from_raw_data(data=data)
            for attr in self._model.to_dict():
                # Remove the attributes from the Model that were not returned
                # This should only be used for Gets with Projection Expressions
                if attr not in data and getattr(self._model, attr, None) is not None:
                    delattr(self._model, attr)
        self._resolved = True

    def get(self):
        if not self._resolved:
            raise InvalidStateError()
        if self._model:
            return self._model
        raise self._model_cls.DoesNotExist()


class Transaction:

    '''
    Base class for a type of transaction operation
    '''

    def __init__(self, connection, return_consumed_capacity: Optional[str] = None) -> None:
        self._connection = connection
        self._return_consumed_capacity = return_consumed_capacity

    def _commit(self):
        raise NotImplementedError()

    def __enter__(self) -> 'Transaction':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and exc_val is None and exc_tb is None:
            self._commit()


class TransactGet(Transaction):

    _results: Optional[List] = None

    def __init__(self, *args, **kwargs):
        self._get_items: List[Dict] = []
        self._futures: List[_ModelFuture] = []
        super(TransactGet, self).__init__(*args, **kwargs)

    def get(self, model_cls, hash_key, range_key=None, attributes_to_get=None):
        '''
        Adds the operation arguments for an item to list of models to get
        returns a _ModelFuture object as a placeholder

        :param model_cls:
        :param hash_key:
        :param range_key:
        :return:
        '''
        operation_kwargs = model_cls.get_operation_kwargs_from_class(
            hash_key, range_key=range_key, attributes_to_get=attributes_to_get, serialize=True, add_identifier_map=True)
        operation_kwargs = {TRANSACT_GET: operation_kwargs}
        model_future = _ModelFuture(model_cls)
        self._futures.append(model_future)
        self._get_items.append(operation_kwargs)
        return model_future

    @staticmethod
    def _update_futures(futures: List[_ModelFuture], results: List) -> None:
        for model, data in zip(futures, results):
            model.update_with_raw_data(data.get(ITEM))

    def _commit(self) -> Any:
        kwargs = {TRANSACT_ITEMS: self._get_items}
        if self._return_consumed_capacity:
            kwargs[RETURN_CONSUMED_CAPACITY] = self._return_consumed_capacity

        response = self._connection.transact_get_items(**kwargs)

        results = response[RESPONSES]

        self._results = results
        self._update_futures(self._futures, results)


class TransactWrite(Transaction):

    def __init__(
        self,
        client_request_token: Optional[str] = None,
        return_item_collection_metrics: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super(TransactWrite, self).__init__(**kwargs)
        self._client_request_token: Optional[str] = client_request_token
        self._return_item_collection_metrics = return_item_collection_metrics
        self._condition_check_items: List[Dict] = []
        self._delete_items: List[Dict] = []
        self._put_items: List[Dict] = []
        self._update_items: List[Dict] = []
        self._models_for_version_attribute_update: List[Any] = []

    def condition_check(self, model_cls, hash_key, range_key=None, condition=None):
        if condition is None:
            raise TypeError('`condition` cannot be None')
        operation_kwargs = model_cls.get_operation_kwargs_from_class(
            hash_key=hash_key,
            range_key=range_key,
            condition=condition,
            serialize=True,
            add_identifier_map=True
        )
        self._condition_check_items.append({TRANSACT_CONDITION_CHECK: operation_kwargs})

    def delete(self, model, condition=None) -> None:
        operation_kwargs = model.get_operation_kwargs_from_instance(condition=condition, serialize=True, add_identifier_map=True)
        self._delete_items.append({TRANSACT_DELETE: operation_kwargs})

    def save(self, model, condition=None, return_values: Optional[str] = None) -> None:
        operation_kwargs = model.get_operation_kwargs_from_instance(
            key=ITEM,
            condition=condition,
            return_values_on_condition_failure=return_values,
            serialize=True,
            add_identifier_map = True
        )
        self._put_items.append({TRANSACT_PUT: operation_kwargs})
        # self._models_for_version_attribute_update.append(model)

    def update(self, model, actions, condition=None, return_values: Optional[str] = None) -> None:
        operation_kwargs = model.get_operation_kwargs_from_instance(
            actions=actions,
            condition=condition,
            return_values_on_condition_failure=return_values,
            serialize=True,
            add_identifier_map=True
        )
        self._update_items.append({TRANSACT_UPDATE: operation_kwargs})
        # self._models_for_version_attribute_update.append(model)

    def _commit(self) -> Any:
        items = self._condition_check_items + self._delete_items + self._put_items + self._update_items
        response = self._connection.transact_write_items(
            TransactItems=items,
            # ClientRequestToken=self._client_request_token,
            # ReturnConsumedCapacity=self._return_consumed_capacity,
            # ReturnItemCollectionMetrics=self._return_item_collection_metrics,
        )
        # for model in self._models_for_version_attribute_update:
        #     model.update_local_version_attribute()
        return response


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
        self.pending_operations.append({ACTION: PUT, ITEM: put_item})

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
        self.pending_operations.append({ACTION: DELETE, ITEM: del_item})

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
            if item[ACTION] == PUT:
                put_items.append(item[ITEM]._serialize()[ATTRIBUTES])
            elif item[ACTION] == DELETE:
                delete_items.append(item[ITEM]._get_keys())
        self.pending_operations = []
        if not delete_items and not put_items:
            return

        table = self.model.resource().Table(self.model.Meta.table_name)
        with table.batch_writer() as batch:
            for item in put_items:
                batch.put_item(Item=item)
            for item in delete_items:
                batch.delete_item(Key=item)
