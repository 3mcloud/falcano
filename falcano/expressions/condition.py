
#pylint: disable=R0903
class Condition():
    ''' Condition class for writing conditions in dynamodb '''
    format_string = ''

    def __init__(self, operator, *values):
        self.operator = operator
        self.values = values

class NotExists(Condition):
    format_string = '{operator} ({0})'

    def __init__(self, path):
        super(NotExists, self).__init__('attribute_not_exists', path)
