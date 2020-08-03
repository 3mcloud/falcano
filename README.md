# Falcano

A Pythonic interface for Amazon's DynamoDB that supports Python 3 and [single-table design](https://www.alexdebrie.com/posts/dynamodb-single-table/) based on [PynamoDB](https://github.com/pynamodb/PynamoDB).

## Installation

```bash
pip install falcano
```

## Basic Usage

Basic usage is nearly identical to `PynamoDB`. `Meta` must inherit from `Model.Meta` and `Type` must be defined for every model.

Create a model that describes a model in your table.

```python
from falcano.model import Model
from falcano.attributes import UnicodeAttribute

class User(Model):
    '''
    A DynamoDB User
    '''
    class Meta(Model.Meta):
        table_name = 'dynamodb-user'
        billing_mode = 'PAY_PER_REQUEST'
    email = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(range_key=True)
    last_name = UnicodeAttribute(hash_key=True)
    Type = UnicodeAttribute(default='user')
```

Create the table if needed:

```python
User.create_table(billing_mode='PAY_PER_REQUEST')
```

Create a new user:

```python
user = User('John', 'Denver')
user.email = 'djohn@company.org'
user.save()
```

Now, search your table for all users with a last name of 'Denver' and whose first name begins with 'J':

```python
for user in User.query('Denver', User.first_name.startswith('J')):
    print(user.first_name)
```

Examples of ways to query your table with filter conditions:

```python
for user in User.query('Denver', User.email.eq('djohn@company.org')):
    print(user.first_name)
for user in User.query('Denver', User.email=='djohn@company.org'):
    print(user.first_name)
```

Retrieve an existing user:

```python
try:
    user = User.get('John', 'Denver')
    print(user)
except User.DoesNotExist:
    print('User does not exist')
```

## Advanced Usage

Indexes? No problem:

```python
from falcano.model import Model
from falcano.indexes import GlobalSecondaryIndex, AllProjection
from falcano.attributes import NumberAttribute, UnicodeAttribute

class ViewIndex(GlobalSecondaryIndex):
    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        projection = AllProjection()
    view = NumberAttribute(default=0, hash_key=True)

class TestModel(Model):
    class Meta(Model.Meta):
        table_name = 'TestModel'
    forum = UnicodeAttribute(hash_key=True)
    thread = UnicodeAttribute(range_key=True)
    view = NumberAttribute(default=0)
    Type = UnicodeAttribute(default='test')
    view_index = ViewIndex()
```

Now query the index for all items with 0 views:

```python
for item in TestModel.view_index.query(0):
    print(f'Item queried from index: {item}')
```

It's simple!

Want to use DynamoDB local? Add a `host` name attribute and specify your local server.

```python
from falcano.models import Model
from falcano.attributes import UnicodeAttribute

class User(Model):
    '''
    A DynamoDB User
    '''
    class Meta(Model.Meta):
        table_name = 'dynamodb-user'
        host = 'http://localhost:8000'
    email = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(range_key=True)
    last_name = UnicodeAttribute(hash_key=True)
    Type = UnicodeAttribute(default='user')
```

## Single-Table Design Usage

Want to follow single-table design with an index and rename the `Type` attribute? No problem:

```python
from falcano.model import Model
from falcano.indexes import GlobalSecondaryIndex, AllProjection
from falcano.attributes import NumberAttribute, UnicodeAttribute

class TypeIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = 'Type'
        billing_mode = 'PAY_PER_REQUEST'
        projection = AllProjection()
    Kind = UnicodeAttribute(default=0, hash_key=True)

class BaseModel(Model):
    class Meta(Model.Meta):
        table_name = 'single_table'
        # use the Kind attribute in place of Type for deserialization
        model_type = 'Kind'
    PK = UnicodeAttribute(hash_key=True)
    SK = UnicodeAttribute(range_key=True)
    TypeIndex = TypeIndex()

class User(BaseModel):
    email = UnicodeAttribute(null=True)
    Kind = UnicodeAttribute(default='user')

class Team(BaseModel):
    owner = UnicodeAttribute(null=True)
    Kind = UnicodeAttribute(default='team')
```

## Features

- Python >= 3.8 support
- Use of `Table` boto3 resource
  - DynamoDB API `conditions` objects
  - Auto-Typing
- An ORM-like interface with query and scan filters
- Compatible with DynamoDB Local
- Support for Unicode, Binary, JSON, Number, Set, and UTC Datetime attributes
- Support for Global and Local Secondary Indexes
- Automatic pagination for bulk operations(?)
- Complex queries
- Multiple models per table and deserialization into objects

## Features not yet implemented

- Provides iterators for working with queries, scans, that are automatically - paginated
- Iterators for working with Query and Scan operations
- Supports the entire DynamoDB API
- Full table backup/restore
- Batch operations with automatic pagination
