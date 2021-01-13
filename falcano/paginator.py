# pylint: disable=unsubscriptable-object
'''
Iterator class for paging through results
'''
from typing import Optional, Union


class Results:
    '''
    ResultIterator handles Query and Scan item pagination.

    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Pagination
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
    '''

    def __init__(self, model, query_response):
        self.records = []
        self.__index: int = 0
        self.model = model
        self.response = query_response
        self.to_models(query_response['Items'])

    def __iter__(self):
        return self

    def __next__(self):
        if self.__index < len(self):
            result = self.records[self.__index]
            self.__index += 1
            return result

        raise StopIteration

    def __len__(self):
        return len(self.records)

    def to_models(self, records):
        '''
        Convert a set of records into Models based on their Type
        '''
        for record in records:
            self.records.append(
                self.model.models[record[self.model.Meta.model_type]](**record))

    def reset(self):
        '''
        Reset index to 0
        '''
        self.__index = 0

    def collection(self, primary_key: Optional[str] = 'PK',
                   sort_key: Optional[str] = 'SK', output: Union[dict, None] = None):
        '''
        Given a list of records from dynamo it will
        iterate and create a dictionary of items
        formatted for the records from the API

        Splits keys on the sort_key param and uses that as the key of the
        records dictionary.

        If output is initially passed it will use the type of the value
        for the key that matches the sort_key prefix. For example, passing
        in {'team':[]} would mean that the records is {'team':[{}]}
        instead of {'team':{}}. This is mostly helpful for ensuring
        that parameters that should return a list even when only a
        single item exists do.

        If the sort_key and primary_key before the first # are equal it assumes this
        item is the primary one and sets the items ID to the primary_key value.
        Otherwise this items sort_key will be the ID
        '''
        if output is None:
            output = {}

        try:
            record = next(self)
        except StopIteration:
            return output

        # Store the sort_key name of this item. We'll use this to know
        # what value to target for the item ID
        id_attr = sort_key
        # Get the items sort key
        key = getattr(record, sort_key)
        item_parts = key.split(record.Meta.separator)
        # This tells us what type of item it is. Presumable all PK are the same, we split on SK
        item_type = item_parts[0]
        tmp_pk = getattr(record, record.get_hash_key().attr_name)
        if item_type == tmp_pk.split(record.Meta.separator)[0]:
            # This is the primary item for the type
            id_attr = primary_key

        # Get the item as a dict, set the ID to id_attr attribute
        item = record.to_dict(primary_key=id_attr, convert_decimal=True)

        # New thing found, make it a dict
        if item_type not in output:
            output[item_type] = item
        # Multiple things exist, convert from dict to list
        elif isinstance(output[item_type], dict):
            output[item_type] = [
                output[item_type],
                item
            ]
        # Thing is a list, append this item to it
        elif isinstance(output[item_type], list):
            output[item_type].append(item)

        # Process the next thing
        return self.collection(
            output=output,
            primary_key=primary_key,
            sort_key=sort_key
        )
