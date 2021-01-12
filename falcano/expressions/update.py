# pylint: disable=too-few-public-methods
'''
Falcano Update Expressions
'''
from typing import List
from falcano.constants import BINARY_SET_SHORT, NUMBER_SET_SHORT, NUMBER_SHORT, STRING_SET_SHORT


class Action:
    '''
    Update Actions
    '''
    format_string = ''

    def __init__(self, *values: 'Path') -> None:
        self.values = values

    def serialize(self, placeholder_names, expression_attribute_values):
        '''serializes the action values'''
        values = [value.serialize(placeholder_names, expression_attribute_values)
                  for value in self.values]
        return self.format_string.format(*values)

    def __repr__(self):
        values = [str(value) for value in self.values]
        return self.format_string.format(*values)


class SetAction(Action):
    """
    The SET action adds an attribute to an item.
    """
    format_string = '{0} = {1}'



class RemoveAction(Action):
    """
    The REMOVE action deletes an attribute from an item.
    """
    format_string = '{0}'



class ListRemoveAction(Action):
    """
    The List REMOVE action deletes an element from a list item based on the index.
    """

    def __init__(self, path: 'Path', *indexes: int):
        self.format_string = ", ".join("{{0}}[{}]".format(index) for index in indexes)
        super().__init__(path)


class AddAction(Action):
    """
    The ADD action appends elements to a set or mathematically adds to a number attribute.
    """
    format_string = '{0} {1}'

    def __init__(self, path: 'Path', subset: 'Path') -> None:
        subset._type_check(BINARY_SET_SHORT, NUMBER_SET_SHORT, NUMBER_SHORT, STRING_SET_SHORT)
        super().__init__(path, subset)


class DeleteAction(Action):
    """
    The DELETE action removes elements from a set.
    """
    format_string = '{0} {1}'

    def __init__(self, path: 'Path', subset: 'Path') -> None:
        subset._type_check(BINARY_SET_SHORT, NUMBER_SET_SHORT, STRING_SET_SHORT)
        super().__init__(path, subset)


class Update:
    '''Update Classn'''

    def __init__(self, *actions: Action) -> None:
        self.set_actions: List[SetAction] = []
        self.remove_actions: List[RemoveAction] = []
        self.add_actions: List[AddAction] = []
        self.delete_actions: List[DeleteAction] = []
        self.list_remove_actions: List[ListRemoveAction] = []
        for action in actions:
            self.add_action(action)

    def add_action(self, action: Action) -> None:
        '''Adds an update action'''
        if isinstance(action, SetAction):
            self.set_actions.append(action)
        elif isinstance(action, RemoveAction):
            self.remove_actions.append(action)
        elif isinstance(action, ListRemoveAction):
            self.list_remove_actions.append(action)
        elif isinstance(action, AddAction):
            self.add_actions.append(action)
        elif isinstance(action, DeleteAction):
            self.delete_actions.append(action)
        else:
            raise ValueError("unsupported action type: '{}'".format(action.__class__.__name__))

    def serialize(self, placeholder_names, expression_attribute_values):
        ''' Serializes an update expression'''
        expression = None
        expression = self._add_clause(
            expression, 'SET',
            self.set_actions,
            placeholder_names,
            expression_attribute_values
        )
        expression = self._add_clause(
            expression,
            'REMOVE',
            self.remove_actions,
            placeholder_names,
            expression_attribute_values
        )
        expression = self._add_clause(
            expression,
            'REMOVE',
            self.list_remove_actions,
            placeholder_names,
            expression_attribute_values
        )
        expression = self._add_clause(
            expression,
            'ADD',
            self.add_actions,
            placeholder_names,
            expression_attribute_values
        )
        expression = self._add_clause(
            expression,
            'DELETE',
            self.delete_actions,
            placeholder_names,
            expression_attribute_values
        )
        return expression

    @staticmethod
    def _add_clause(expression, keyword, actions, placeholder_names, expression_attribute_values):
        clause = Update._get_clause(keyword, actions, placeholder_names,
                                    expression_attribute_values)
        if clause is None:
            return expression
        return clause if expression is None else expression + " " + clause

    @staticmethod
    def _get_clause(keyword, actions, placeholder_names, expression_attribute_values):
        actions = ", ".join(
            [
                action.serialize(placeholder_names, expression_attribute_values)
                for action in actions
            ]
        )
        return keyword + " " + actions if actions else None
