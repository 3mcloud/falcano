# Falcano DB Batch Operations

Falcano supports batch writes and reads!
These are particularly useful if you are loading or fetching large quantities of data to/from DynamoDB.

Suppose that you are working for a bank who wants you to transfer both credit card information, and account transaction data, to a single table in DynamoDB. 
You are given 2 CSV files, where the credit card info data is in the following format in the first file:

```csv
Card Type,Issuing Bank,Card Number,Card Holder's Name,CVV/CVV2,Issue Date,Expiry Date,Billing Date,Card PIN,Credit Limit
------------------------------------------------------------------------------------------------------------------------
Visa,Chase,4431465245886276,Frank Q Ortiz,362,09/2016,09/2034,7,1247,103700
Discover,Discover,6224764404044446,Tony E Martinez,035,06/2012,06/2030,23,6190,92900
```
and the transaction data is in the following format in the second file:

```csv
TransactionID,Account,Date,Description,Deposits,Withdrawls,Balance
------------------------------------------------------------------
d7e48413-7900-48af-8b21-347ed190ae01,5649170083,20-Aug-2020,Cheque,3391.02,00.00,83839.30
560b5bbb-e604-4b3a-9a9b-3367dc0937b3,4272472908,20-Aug-2020,ATM,147844.20,00.00,231683.50
```

Let's begin with a base model that includes a type gsi, then we can extend it to make our bank data models.

```python
import os
import csv
import datetime
import stringcase
from decimal import Decimal
from falcano.model import Model
from falcano.indexes import GlobalSecondaryIndex, AllProjection
from falcano.attributes import UnicodeAttribute, NumberAttribute, UTCDateTimeAttribute
from falcano.exceptions import DoesNotExist

os.environ['DYNAMODB_TABLE'] = 'bank-table'
os.environ['AWS_ACCESS_KEY_ID'] = 'my-access-key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'my-secret-key'


class TypeIndex(GlobalSecondaryIndex):
    """ Type Index """
    class Meta: # pylint: disable=too-few-public-methods
        """ GSI properties """
        index_name = 'type'
        projection = AllProjection()
    Type = UnicodeAttribute(default='bank_transaction', hash_key=True)
    sk = UnicodeAttribute(range_key=True)


class BaseModel(Model):
    '''Base model with meta'''
    class Meta(Model.Meta):
        ''' Table properties '''
        table_name = os.environ.get('DYNAMODB_TABLE')
        model_type = 'type'
    PK = UnicodeAttribute(hash_key=True)
    SK = UnicodeAttribute(range_key=True)
    TypeIndex = TypeIndex()
```

Now, lets create a couple of models for our 2 different types of bank data: BankTransaction and CreditCard.

```python
class BankTransaction(BaseModel):
    '''
    Banking Transaction Type
    PK: bank_transaction#trasaction_id
    SK: bank_transaction#account_number
    '''
    Type = UnicodeAttribute(default='bank_transaction')
    TransactionId = UnicodeAttribute()
    AccountNumber = NumberAttribute()
    Date = UTCDateTimeAttribute()
    Description = UnicodeAttribute()
    Deposits = NumberAttribute()
    Withdrawals = NumberAttribute()
    Balance = NumberAttribute()
    DoesNotExist = DoesNotExist


class CreditCard(BaseModel):
    '''
    Credit Card Type
    PK: credit_card#card_number
    SK: credit_card#card_type
    '''
    Type = UnicodeAttribute(default='credit_card')
    CardType = UnicodeAttribute()
    IssuingBank = UnicodeAttribute()
    CardNumber = NumberAttribute()
    CardHolderName = UnicodeAttribute()
    CVV = NumberAttribute()
    IssueDate = UTCDateTimeAttribute()
    ExpiryDate = UTCDateTimeAttribute()
    BillingDate = NumberAttribute()
    PIN = NumberAttribute()
    CreditLimit = NumberAttribute()
    DoesNotExist = DoesNotExist

```
If the table doesn't already exist, we need to create it before batch inserting items. 

```python
BaseModel.create_table(wait=True)
```
From here, we will use the csv library, in conjunction with the Falcano batch writer, to add a record for each row of each CSV file to a single batch, and commit all of this data to DynamoDB. 

```python
batch_writer = BaseModel.batch_write()

with open('cc.csv', newline='') as f:
    reader = csv.reader(f)
    next(reader) # skip header
    for row in reader:
        batch_writer.save(
            CreditCard(
                'credit_card#'+stringcase.snakecase(row[2]),
                'credit_card#'+stringcase.snakecase(row[0]),
                CardType=row[0],
                IssuingBank=row[1],
                CardNumber=int(row[2]),
                CardHolderName=row[3],
                CVV=int(row[4]),
                IssueDate=datetime.datetime.strptime(row[5], '%m/%Y'),
                ExpiryDate=datetime.datetime.strptime(row[6], '%m/%Y'),
                BillingDate=int(row[7]),
                PIN=int(row[8]),
                CreditLimit=int(row[9])
            )
        )

with open('bt.csv', newline='') as f:
    reader = csv.reader(f)
    next(reader) # skip header
    for row in reader:
        batch_writer.save(
            BankTransaction(
                'bank_transaction#'+stringcase.snakecase(row[0]),
                'bank_transaction#'+stringcase.snakecase(row[1]),
                TransactionId=row[0],
                AccountNumber=int(row[1]),
                Date=datetime.datetime.strptime(row[2], '%d-%b-%Y'),
                Description=row[3],
                Deposits=Decimal(row[4]),
                Withdrawals=Decimal(row[5]),
                Balance=Decimal(row[6]),
            )
        )

batch_writer.commit()
```

Now that we have inserted all of the data from both files, we can use Falcanos batch reader to fetch large quantities of this data, using the PKs and SKs of the items to fetch. 
 
```python
items = [
    ('credit_card#4431465245886276', 'credit_card#visa'),
    ('credit_card#6224764404044446', 'credit_card#discover'),
    ('bank_transaction#d7e48413-7900-48af-8b21-347ed190ae01', 'bank_transaction#5649170083'),
    ('bank_transaction#560b5bbb-e604-4b3a-9a9b-3367dc0937b3', 'bank_transaction#4272472908'),
    ...
]


records = BaseModel.batch_get(items)

print(records.collection())

```
