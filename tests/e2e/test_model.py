from datetime import datetime
import pytest
from falcano.model import Model
from falcano.attributes import UnicodeAttribute

# @pytest.mark.ddblocal
def test_model_integration(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'fakeMyKeyId')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'fakeMySecret')
    class TestModel(Model):
        """
        A model for testing
        """
        class Meta(Model.Meta):
            region = 'us-east-1'
            table_name = 'falcano-e2e'
            billing_mode = 'PAY_PER_REQUEST'
        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)

    if not TestModel.exists():
        print('Creating table')
        TestModel.create_table(wait=True)


    obj = TestModel('1', '2')
    obj.save()
    obj.delete()

    # obj.refresh()
    # obj = TestModel('foo', 'bar')
    # obj.save()
    # TestModel('foo2', 'bar2')
    # obj3 = TestModel('setitem', 'setrange', scores={1, 2.1})
    # obj3.save()
    # obj3.refresh()

# class User(Model):
#     '''
#     A DynamoDB User
#     '''
#     class Meta(Model.Meta):
#         table_name = 'dynamodb-user'
#         billing_mode = 'PAY_PER_REQUEST'
#     email = UnicodeAttribute(null=True)
#     first_name = UnicodeAttribute(range_key=True)
#     last_name = UnicodeAttribute(hash_key=True)
#     Type = UnicodeAttribute(default='user')
# ```
#
# Create the table if needed:
#
# ```python
# User.create_table(billing_mode='PAY_PER_REQUEST')
# ```
#
# Create a new user:
#
# ```python
# user = User('John', 'Denver')
# user.email = 'djohn@company.org'
# user.save()
