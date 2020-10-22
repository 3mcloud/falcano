# Falcano DB Transactions

Falcano supports transaction gets and transaction writes!

Let's start with a base model that includes a type gsi, then we can extend it to make other models.


```python
import os
from falcano.model import Model
from falcano.indexes import GlobalSecondaryIndex, AllProjection
from falcano.attributes import UnicodeAttribute, NumberAttribute
from falcano.exceptions import DoesNotExist

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
    Type = UnicodeAttribute(default='person', hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    
class BaseModel(Model):
    '''Base model with meta'''
    class Meta(Model.Meta):
        ''' Table properties '''
        table_name = os.environ.get('DYNAMODB_TABLE')
    PK = UnicodeAttribute(hash_key=True)
    SK = UnicodeAttribute(range_key=True)
    TypeIndex = TypeIndex()
```

Now, lets create a couple of types, Person and Pet

```python
class Person(BaseModel):
    '''
    Hackathon Projects
    PK: person#uuid
    SK: person#casedname
    '''
    Type = UnicodeAttribute(default='person')
    FirstName = UnicodeAttribute()
    LastName = UnicodeAttribute()
    Age = NumberAttribute()
    DoesNotExist = DoesNotExist


class Pet(BaseModel):
    '''
    Hackathon Projects
    PK: person#uuid
    SK: person#casedname
    '''
    Type = UnicodeAttribute(default='pet')
    Name = UnicodeAttribute()
    OwnerName = UnicodeAttribute()
    Age = NumberAttribute()
    DoesNotExist = DoesNotExist

```
If the table doesn't already exist, make sure you create it before trying to insert items. Just comment this out if you already have a table created.

```python
BaseModel.create_table(wait=True)
```
From here, we will use falcano to create 2 people (Steve and Carol) and their pets (Fido and Sparky) in a single transaction. 

```python
steve_smith = Person(
    "person#1234", # this is the PK, or 'Primary Key'
    "person#stevesmith", # this is the SK, or 'Range Key'
    FirstName="Steve",
    LastName="Smith",
    Age=20,
)

fido = Pet(
    "pet#1234",
    "pet#fido",
    Name="Fido",
    OwnerName="Steve Smith",
    Age=2
)

carol_cramer = Person(
    "person#5678", 
    "person#carolcramer",
    FirstName="Carol",
    LastName="Cramer",
    Age=40,
)

sparky = Pet(
    "pet#5678",
    "pet#sparky",
    Name="Sparky",
    OwnerName="Carol Cramer",
    Age=4
)

with BaseModel.transact_write() as writer:
    writer.save(steve_smith)
    writer.save(fido)
    writer.save(carol_cramer)
    writer.save(sparky)

with BaseModel.transact_get() as getter:
    got_steve = getter.get(Person, "person#1234", "person#stevesmith")
    got_fido = getter.get(Pet, "pet#1234", "pet#fido")
    got_carol = getter.get(Person,"person#5678", "person#carolcramer")
    got_sparky = getter.get(Pet, "pet#5678", "pet#sparky")

print(got_steve.get().to_dict())
print(got_fido.get().to_dict())
print(got_carol.get().to_dict())
print(got_sparky.get().to_dict())
```

However, we can do even more operations in a transaction, such as updates and deletes. Conditions can also be added to the entire transaction, in the same manner one could be added to an individual operation.
Suppose that on Fido's 3rd birthday, his owner Steve passes away :( and Carol tries to becomes his new owner. But Fido will only accept this change of ownership if he gets a new pet-brother out of the process.
 
```python
with BaseModel.transact_write() as writer:
    writer.condition_check(Pet, "pet#5678", "pet#sparky", Pet.OwnerName.eq("Carol Cramer"))
    action_1 = Pet.Age.add(1)
    action_2 = Pet.OwnerName.set("Carol Cramer")
    writer.update(fido, [action_1, action_2])
    writer.delete(steve_smith)


with BaseModel.transact_get() as getter:
    got_carol = getter.get(Person,"person#5678", "person#carolcramer")
    got_sparky = getter.get(Pet, "pet#5678", "pet#sparky")
    got_fido = getter.get(Pet, "pet#1234", "pet#fido")
    got_steve = getter.get(Person, "person#1234", "person#stevesmith")

print("Transact Write/Get Results (advanced):")
print(got_carol.get().to_dict())
print(got_sparky.get().to_dict())
print(got_fido.get().to_dict())
print(got_steve.get().to_dict())

```
