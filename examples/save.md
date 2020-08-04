# Falcano DB Save

Falcano allows saving! Duh.

Let's start with a base model, then we can extend it to make other models.

```python
from falcano.model import Model
from falcano.attributes import UnicodeAttribute

class BaseModel(Model):
    '''Base model with meta'''
    class Meta(Model.Meta):
        ''' Table properties '''
        table_name = os.environ.get('DYNAMODB_TABLE')
        billing_mode = 'PAY_PER_REQUEST'
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
    Description = UnicodeAttribute(null=True)
    VideoUrl = UnicodeAttribute(null=True)
    Status = UnicodeAttribute(null=True)
    Year = UnicodeAttribute()
    Location = UnicodeAttribute(default='US')

```