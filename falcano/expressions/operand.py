# pylint: disable=unsubscriptable-object
'''
Falcano operands
'''
from typing import Any, Dict, List, Optional, Union

from falcano.constants import (
    ATTR_TYPE_MAP, LIST, LIST_SHORT, MAP, MAP_SHORT,
    NUMBER_SHORT, SHORT_ATTR_TYPES
)
from falcano.expressions.update import (
    AddAction, DeleteAction, RemoveAction, SetAction, ListRemoveAction
)
from falcano.expressions.util import get_path_segments, get_value_placeholder, substitute_names

_PathOrAttribute = Union['Path', 'Attribute', List[str], str]


class _Operand:
    """
    Operand is the base class for objects that can be operands in Update Expressions.
    """
    format_string = ''
    short_attr_type: Any = None

    def __init__(self, *values: Any) -> None:
        self.values = values

    def __repr__(self) -> str:
        return self.format_string.format(*self.values)

    def serialize(self,
                  placeholder_names: Dict[str, str],
                  expression_attribute_values: Dict[str, str]
                  ) -> str:
        ''' Serializes values '''
        values = [self._serialize_value(value, placeholder_names,
                                        expression_attribute_values) for value in self.values]
        return self.format_string.format(*values)

    def _to_operand(self, value: Union['_Operand', 'Attribute', Any]):
        if isinstance(value, _Operand):
            return value
        # prevent circular import -- Attribute imports Path
        from falcano.attributes import Attribute, MapAttribute # pylint: disable=import-outside-toplevel
        if isinstance(value, MapAttribute) and value.is_attribute_container():
            return self._to_value(value)
        return Path(value) if isinstance(value, Attribute) else self._to_value(value)

    def _type_check(self, *types):
        if self.short_attr_type and self.short_attr_type not in types:
            raise ValueError("The data type of '{}' must be one of {}".format(self, list(types)))

    @classmethod
    def _serialize_value(cls, value, placeholder_names, expression_attribute_values):
        return value.serialize(placeholder_names, expression_attribute_values)

    @classmethod
    def _to_value(cls, value):
        return Value(value)


class _NumericOperand(_Operand):
    """
    A base class for Operands that can be used in the
    increment and decrement SET update actions.
    """

    def __add__(self, other):
        return Increment(self, self._to_operand(other))

    def __radd__(self, other):
        return Increment(self._to_operand(other), self)

    def __sub__(self, other):
        return Decrement(self, self._to_operand(other))

    def __rsub__(self, other):
        return Decrement(self._to_operand(other), self)


class _ListAppendOperand(_Operand):
    """
    A base class for Operands that can be used in the
    list_append function for the SET update action.
    """

    def append(self, other: Any) -> 'ListAppend':
        '''List append'''
        return ListAppend(self, self._to_operand(other))

    def prepend(self, other: Any) -> 'ListAppend':
        '''List prepend'''
        return ListAppend(self._to_operand(other), self)


class Increment(_Operand):  # pylint: disable=too-few-public-methods
    """
    Increment is a special operand that represents an increment SET update action.
    """
    format_string = '{0} + {1}'
    short_attr_type = NUMBER_SHORT

    def __init__(self, lhs: '_Operand', rhs: '_Operand') -> None:
        lhs._type_check(NUMBER_SHORT)
        rhs._type_check(NUMBER_SHORT)
        super().__init__(lhs, rhs)


class Decrement(_Operand): # pylint: disable=too-few-public-methods
    """
    Decrement is a special operand that represents an decrement SET update action.
    """
    format_string = '{0} - {1}'
    short_attr_type = NUMBER_SHORT

    def __init__(self, lhs: _Operand, rhs: _Operand) -> None:
        lhs._type_check(NUMBER_SHORT)
        rhs._type_check(NUMBER_SHORT)
        super().__init__(lhs, rhs)


class ListAppend(_Operand): # pylint: disable=too-few-public-methods
    """
    ListAppend is a special operand that represents the
    list_append function for the SET update action.
    """
    format_string = 'list_append ({0}, {1})'
    short_attr_type = LIST_SHORT

    def __init__(self, list1: _Operand, list2: _Operand):
        list1._type_check(LIST_SHORT)
        list2._type_check(LIST_SHORT)
        super().__init__(list1, list2)


class IfNotExists(_NumericOperand, _ListAppendOperand):
    """
    IfNotExists is a special operand that represents the
    if_not_exists function for the SET update action.
    """
    format_string = 'if_not_exists ({0}, {1})'

    def __init__(self, path: _Operand, value: Any) -> None:
        self.short_attr_type = path.short_attr_type or value.short_attr_type
        if self.short_attr_type != value.short_attr_type:
            # path and value have conflicting types -- defer any type checks to DynamoDB
            self.short_attr_type = None
        super().__init__(path, value)


class Value(_NumericOperand, _ListAppendOperand):
    """
    Value is an operand that represents an attribute value.
    """
    format_string = '{0}'

    def __init__(self, value: Any, attribute: Optional['Attribute'] = None) -> None:
        # Check to see if value is already serialized
        if isinstance(value, dict) and len(value) == 1 and list(value.keys())[0] in SHORT_ATTR_TYPES: # pylint: disable=line-too-long
            (self.short_attr_type, value), = value.items()
        elif value is None:
            (self.short_attr_type, value) = Value.__serialize(value)
        else:
            (self.short_attr_type, value) = Value.__serialize(value, attribute)
        # if isinstance(value, set):  # self.short_attr_type in [STRING_SET, MAP_SHORT]:
        #     super(Value, self).__init__({self.short_attr_type: value})
        super().__init__(value)

    @property
    def value(self):
        '''Get the value'''
        return self.values[0]

    def _serialize_value(self, value, placeholder_names, expression_attribute_values):
        return get_value_placeholder(value, expression_attribute_values)

    @staticmethod
    def __serialize(value, attribute=None):
        if attribute is None:
            return Value.__serialize_based_on_type(value)
        if attribute.attr_type == LIST and not isinstance(value, list):
            # List attributes assume the values to be serialized are lists.
            (attr_type, attr_value), = attribute.serialize([value])[0].items()
            return attr_type, attr_value
        if attribute.attr_type == MAP and not isinstance(value, dict):
            # Map attributes assume the values to be serialized are maps.
            return Value.__serialize_based_on_type(value)
        return ATTR_TYPE_MAP[attribute.attr_type], attribute.serialize(value)

    @staticmethod
    def __serialize_based_on_type(value):
        from falcano.attributes import _get_class_for_serialize  # pylint: disable=import-outside-toplevel
        attr_class = _get_class_for_serialize(value)
        return ATTR_TYPE_MAP[attr_class.attr_type], attr_class.serialize(value)


class Path(_NumericOperand, _ListAppendOperand):
    """
    Path is an operand that represents either an attribute name or document path.
    """
    format_string = '{0}'

    def __init__(self, attribute_or_path: _PathOrAttribute) -> None:
        # prevent circular import -- Attribute imports Path
        from falcano.attributes import Attribute  # pylint: disable=import-outside-toplevel
        path: _PathOrAttribute
        if isinstance(attribute_or_path, Attribute):
            self.attribute = attribute_or_path
            self.short_attr_type = ATTR_TYPE_MAP[attribute_or_path.attr_type]
            path = attribute_or_path.attr_path
        else:
            self.attribute = None
            self.short_attr_type = None
            path = attribute_or_path
        if not path:
            raise ValueError("path cannot be empty")
        super().__init__(get_path_segments(path))

    @property
    def path(self) -> Any:
        '''Get the path'''
        return self.values[0]

    def __iter__(self):
        # Because we define __getitem__ Path is considered an iterable
        raise TypeError("'{}' object is not iterable".format(self.__class__.__name__))

    def __getitem__(self, item: Union[int, str]) -> 'Path':
        # The __getitem__ call returns a new Path instance without any attribute set.
        # This is intended since the nested element is not the same attribute as ``self``.
        if self.attribute and self.attribute.attr_type not in [LIST, MAP]:
            raise TypeError("'{}' object has no attribute __getitem__".format(
                self.attribute.__class__.__name__))
        if self.short_attr_type == LIST_SHORT and not isinstance(item, int):
            raise TypeError("list indices must be integers, not {}".format(type(item).__name__))
        if self.short_attr_type == MAP_SHORT and not isinstance(item, str):
            raise TypeError("map attributes must be strings, not {}".format(type(item).__name__))
        if isinstance(item, int):
            # list dereference operator
            element_path = Path(self.path)  # copy the document path before indexing last element
            element_path.path[-1] = '{}[{}]'.format(self.path[-1], item)
            return element_path
        if isinstance(item, str):
            # map dereference operator
            return Path(self.path + [item])
        raise TypeError("item must be an integer or string, not {}".format(type(item).__name__))

    def __or__(self, other):
        return IfNotExists(self, self._to_operand(other))

    def set(self, value: Any) -> SetAction:
        ''' Returns an update action that sets this attribute to the given value '''
        return SetAction(self, self._to_operand(value))

    def remove(self) -> RemoveAction:
        ''' Returns an update action that removes this attribute from the item '''
        return RemoveAction(self)

    def remove_list_elements(self, *indexes: int) -> ListRemoveAction:
        ''' Returns an update action that removes list element(s) '''
        return ListRemoveAction(self, *indexes)

    def add(self, *values: Any) -> AddAction:
        '''Returns an update action that appends the given values to a set
           or mathematically adds a value to a number'''
        value = values[0] if len(values) == 1 else values
        return AddAction(self, self._to_operand(value))

    def delete(self, *values: Any) -> DeleteAction:
        ''' Returns an update action that removes the given values from a set attribute '''
        value = values[0] if len(values) == 1 else values
        return DeleteAction(self, self._to_operand(value))

    def _serialize_value(self, value, placeholder_names, expression_attribute_values):
        return substitute_names(value, placeholder_names)

    def _to_value(self, value: Any) -> Value:
        return Value(value, attribute=self.attribute)

    def __str__(self) -> str:
        # Quote the path to illustrate that any dot characters are not dereference operators.
        quoted_path = [self._quote_path(
            segment) if '.' in segment else segment for segment in self.path]
        return '.'.join(quoted_path)

    def __repr__(self) -> str:
        return "Path({})".format(self.path)

    @staticmethod
    def _quote_path(path):
        path, sep, rem = path.partition('[')
        return repr(path) + sep + rem
