# pylint: disable=unsubscriptable-object
'''
Falcano exceptions
'''
from typing import Optional


class FalcanoException(Exception):
    '''
    A common exception class
    '''

    def __init__(self, msg: Optional[str] = None, cause: Optional[Exception] = None) -> None:
        self.msg = msg
        self.cause = cause
        super().__init__(self.msg)

    @property
    def cause_response_code(self) -> Optional[str]:
        '''
        Gets the response error code
        '''
        return getattr(self.cause, 'response', {}).get('Error', {}).get('Code')

    @property
    def cause_response_message(self) -> Optional[str]:
        '''
        Gets the response error message
        '''
        return getattr(self.cause, 'response', {}).get('Error', {}).get('Message')


class TableDoesNotExist(FalcanoException):
    '''
    Raised when an operation is attempted on a table that doesn't exist
    '''

    def __init__(self, table_name: str) -> None:
        msg = f"Table does not exist: `{table_name}`"
        super().__init__(msg)


class DoesNotExist(FalcanoException):
    '''
    Raised when an item queried does not exist
    '''

    def __init__(self) -> None:
        msg = 'Item does not exist'
        super().__init__(msg)


class InvalidStateError(FalcanoException):
    '''
    Raises when the internal state of an operation context is invalid
    '''
    msg = 'Operation in invalid state'
