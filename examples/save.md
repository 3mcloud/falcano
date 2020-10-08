# Falcano DB Save

Falcano allows saving!

Let's start with a base model that includes a type gsi, then we can extend it to make other models.


```python
import os
from falcano.model import Model
from falcano.indexes import GlobalSecondaryIndex, AllProjection
from falcano.attributes import UnicodeAttribute

# These are the environment variables needed for this example
os.environ['DYNAMODB_TABLE'] = 'my-table-name'
os.environ['AWS_ACCESS_KEY_ID'] = 'my-access-key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'my-secret-key'

class TypeIndex(GlobalSecondaryIndex):
    """ Type Index """
    class Meta: # pylint: disable=too-few-public-methods
        """ GSI properties """
        index_name = 'Type'
        projection = AllProjection()
    Type = UnicodeAttribute(default='project', hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    
class BaseModel(Model):
    '''Base model with meta'''
    class Meta(Model.Meta):
        ''' Table properties '''
        table_name = os.environ.get('DYNAMODB_TABLE')
    PK = UnicodeAttribute(hash_key=True)
    SK = UnicodeAttribute(range_key=True)
    TypeIndex = TypeIndex()

class Project(BaseModel):
    '''
    Hackathon Projects
    PK: project#uuid
    SK: project#casedname
    '''
    Type = UnicodeAttribute(default='project')
    Name = UnicodeAttribute()

```
If the table doesn't already exist, make sure you create it before trying to insert items. Just comment this out if you already have a table created.

```python
BaseModel.create_table(wait=True)
```
From here, we will use falcano to create an item in dynamodb.

```python
proj = Project(
    "project#1234", # this is the PK, or 'Primary Key'
    "project#myprojectname",
    Name="My Project Name"
)
proj.save()
# project is saved!

# now get the project
returned_project = Project.get(
    proj.PK,
    proj.SK
)

print(returned_project.to_dict())
```
