# pylint: disable=unsubscriptable-object
'''
Index support
'''
from typing import Any, Optional, Dict, List
from inspect import getmembers

from boto3.dynamodb.conditions import ComparisonCondition
from falcano.constants import (
    ALL, ATTR_NAME, ATTR_TYPE, KEY_TYPE, ATTR_TYPE_MAP, KEY_SCHEMA,
    ATTR_DEFINITIONS, META_CLASS_NAME
)

from falcano.types import HASH, RANGE
from falcano.attributes import Attribute

_KeyType = Any


class IndexMeta(type):
    '''
    Index meta class
    This class is here to allow for an index `Meta` class
    that contains the index settings
    '''

    def __init__(cls, name, bases, attrs, *args, **kwargs):
        super().__init__(name, bases, attrs, *args, **kwargs)  # type: ignore
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == META_CLASS_NAME:
                    meta_cls = attrs.get(META_CLASS_NAME)
                    if meta_cls is not None:
                        meta_cls.attributes = None
                elif isinstance(attr_obj, Attribute):
                    if attr_obj.attr_name is None:
                        attr_obj.attr_name = attr_name


class Index(metaclass=IndexMeta):
    '''
    Base class for secondary indexes
    '''
    Meta: Any = None

    def __init__(self) -> None:
        if self.Meta is None:
            raise ValueError('Indexes require a Meta class for settings')
        if not hasattr(self.Meta, 'projection'):
            raise ValueError('No projection defined, define a projection for this class')

    @classmethod
    def count(  # pylint: disable=too-many-arguments
            cls,
            hash_key: _KeyType,
            range_key_condition: Optional[ComparisonCondition] = None,
            filter_condition: Optional[ComparisonCondition] = None,
            consistent_read: bool = False,
            limit: Optional[int] = None,
            rate_limit: Optional[float] = None,
    ) -> int:
        '''
        Count on an index
        '''
        return cls.Meta.model.count(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=cls.Meta.index_name,
            consistent_read=consistent_read,
            limit=limit,
            rate_limit=rate_limit,
        )

    @classmethod
    def query(  # pylint: disable=too-many-arguments
            cls,
            hash_key: _KeyType,
            range_key_condition: Optional[ComparisonCondition] = None,
            filter_condition: Optional[ComparisonCondition] = None,
            consistent_read: Optional[bool] = False,
            scan_index_forward: Optional[bool] = None,
            limit: Optional[int] = None,
            last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
            attributes_to_get: Optional[List[str]] = None,
            page_size: Optional[int] = None,
    ):
        '''
        Queries an index
        '''
        return cls.Meta.model.query(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            consistent_read=consistent_read,
            index_name=cls,
            scan_index_forward=scan_index_forward,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            page_size=page_size,
        )

    @classmethod
    def scan(  # pylint: disable=too-many-arguments
            cls,
            filter_condition: Optional[ComparisonCondition] = None,
            segment: Optional[int] = None,
            total_segments: Optional[int] = None,
            limit: Optional[int] = None,
            last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
            page_size: Optional[int] = None,
            consistent_read: Optional[bool] = None,
            rate_limit: Optional[float] = None,
            attributes_to_get: Optional[List[str]] = None,
    ):
        '''
        Scans an index
        '''
        return cls.Meta.model.scan(
            filter_condition=filter_condition,
            segment=segment,
            total_segments=total_segments,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            page_size=page_size,
            consistent_read=consistent_read,
            index_name=cls.Meta.index_name,
            rate_limit=rate_limit,
            attributes_to_get=attributes_to_get,
        )

    @classmethod
    def hash_key_attribute(cls):
        '''
        Returns the attribute class for the hash key
        '''
        for attr_cls in cls._get_attributes().values():
            if attr_cls.is_hash_key:
                return attr_cls
        return None

    @classmethod
    def get_schema(cls) -> Dict:
        '''
        Returns the schema for this index
        '''
        attr_definitions = []
        schema = []
        for _, attr_cls in cls._get_attributes().items():
            attr_definitions.append({
                ATTR_NAME: attr_cls.attr_name,
                ATTR_TYPE: ATTR_TYPE_MAP[attr_cls.attr_type]
            })
            if attr_cls.is_hash_key:
                # Ensure the hash key is the first item in the list
                schema.insert(0, {
                    ATTR_NAME: attr_cls.attr_name,
                    KEY_TYPE: HASH
                })
            elif attr_cls.is_range_key:
                schema.append({
                    ATTR_NAME: attr_cls.attr_name,
                    KEY_TYPE: RANGE
                })
        return {
            KEY_SCHEMA: schema,
            ATTR_DEFINITIONS: attr_definitions
        }

    @classmethod
    def _get_attributes(cls):
        '''
        Returns the list of attributes for this class
        '''
        if cls.Meta.attributes is None:
            cls.Meta.attributes = {}
            for name, attribute in getmembers(cls, lambda o: isinstance(o, Attribute)):
                cls.Meta.attributes[name] = attribute
        return cls.Meta.attributes


class GlobalSecondaryIndex(Index):
    '''
    A global secondary index
    '''


class LocalSecondaryIndex(Index):
    '''
    A local secondary index
    '''


class Projection():  # pylint: disable=too-few-public-methods
    '''
    A class for presenting projections
    '''
    projection_type: Any = None
    non_key_attributes: Any = None


class AllProjection(Projection):  # pylint: disable=too-few-public-methods
    '''
    An ALL projection
    '''
    projection_type = ALL
