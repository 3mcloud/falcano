# Falcano DB Save

Falcano allows saving!

Let's start with a base model, then we can extend it to make other models.


```python
from falcano.model import Model
from falcano.attributes import UnicodeAttribute

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

