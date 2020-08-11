from typing import Any, Optional

import botocore.exceptions


class FalcanoException(Exception):
    '''
    A common exception class
    '''

    def __init__(self, msg: Optional[str] = None, cause: Optional[Exception] = None) -> None:
        self.msg = msg
        self.cause = cause
        super(FalcanoException, self).__init__(self.msg)

    @property
    def cause_response_code(self) -> Optional[str]:
        return getattr(self.cause, 'response', {}).get('Error', {}).get('Code')

    @property
    def cause_response_message(self) -> Optional[str]:
        return getattr(self.cause, 'response', {}).get('Error', {}).get('Message')


class TableDoesNotExist(FalcanoException):
    '''
    Raised when an operation is attempted on a table that doesn't exist
    '''

    def __init__(self, table_name: str) -> None:
        msg = f"Table does not exist: `{table_name}`"
        super(TableDoesNotExist, self).__init__(msg)


class DoesNotExist(FalcanoException):
    '''
    Raised when an item queried does not exist
    '''

    def __init__(self) -> None:
        msg = 'Item does not exist'
        super(DoesNotExist, self).__init__(msg)
