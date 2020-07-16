
'''
Global Utilities
'''

def snake_to_camel_case(var_name: str) -> str:
    '''
    Converts camel case variable names to snake case variable_names
    '''
    first, *others = var_name.split('_')
    return ''.join([first.lower(), *map(str.title, others)])
