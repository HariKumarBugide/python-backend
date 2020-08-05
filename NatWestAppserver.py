from flask import *
from pymongo import MongoClient
import pandas as pd


app = Flask(__name__)
app.secret_key='N@twestkey1_2907!'
authenticated = True

client = MongoClient("mongodb+srv://natwest_user:NatwestDxcMongo@natwestdb-x3meq.azure.mongodb.net/test")  # host uri
usercredentialsdb = client.credentials
usercredentialscollection = usercredentialsdb.rbs_credentials
usercategorizationdb = client.aggregated_data
usercategorizationcollection = usercategorizationdb.unique_categories_mvp_1
transactionsdb = client.Repro_DB
usertransactioncollection = transactionsdb.Repro_RBS

@app.route('/')
def home():
    return "welcome to natwest backend"

@app.route('/home')
def home1():
    return "welcome to natwest backend"

@app.route('/auth')
def auth():
    # read in username and password
    username = request.args['username']
    password = request.args['password']

    u = usercredentialscollection.aggregate([
        {'$unwind': '$UserData'},
        {'$match': {
            'UserData.User': username,
            'UserData.Password': password
        }},
        {'$project': {
            '_id': 0, "UserData.User": 1, "UserData.AccountId": 1, "UserData.Password": 1
        }
        },
    ])
    credentials = list(u)
    try:
        a = credentials[0]
        b = a['UserData']
        c = b['AccountId']
        session['user'] = username
        if username == b['User'] and password == b['Password']:
            result = "Authenticated"
            session['auth'] = True

            response = {result : c}
        else:
            result = "Not Authenticated"
            session['auth'] = False
            c = "User account not found or invalid credentials - no account number"
            response = {result : c}

        return response
    except IndexError:
        return "User account not found or invalid credentials - no account number"


@app.route('/transactions', methods=['GET', 'POST'])
def transactions():
    userid = request.args['accountid']
    response = transactions_by_userid(userid)
    return response

def transactions_by_userid(userid):
    t = usertransactioncollection.aggregate([
        {'$unwind': '$TransactionData.Data.Transaction'},
        {'$match': {
            'TransactionData.Data.Transaction.AccountId': userid,
        }},
        {'$project': {
            'TransactionData.Data.Transaction': 1, '_id': 0
        }

        },
    ])
    Transaction = list(t)

    Totaltransaction = []

    for i in Transaction:
        Totaltransaction.append(i["TransactionData"]["Data"]["Transaction"])

    AccountId = []
    TransactionCategory = []
    Amount = []
    Account_Balance = []
    TransactionType = []
    TransactionDate = []

    for i in range(len(Totaltransaction)):
        AccountId.append(Totaltransaction[i]['AccountId'])
        TransactionCategory.append(Totaltransaction[i]['TransactionCategory'])
        Amount.append(Totaltransaction[i]['Amount']['Amount'])
        Account_Balance.append(Totaltransaction[i]['Balance']['Amount']['Amount'])
        TransactionType.append(Totaltransaction[i]['CreditDebitIndicator'])
        TransactionDate.append(Totaltransaction[i]['BookingDateTime'])

    df_totaltransaction = pd.DataFrame({'AccountId': AccountId, 'TransactionCategory': TransactionCategory, 'Amount': Amount,'Balance': Account_Balance, 'TransactionType': TransactionType, 'TransactionDate': TransactionDate},columns=['AccountId', 'TransactionCategory', 'Amount', 'Balance', 'TransactionType', 'TransactionDate'])
    a = df_totaltransaction.to_json()

    return a

@app.route('/accounts', methods=['GET', 'POST'])
def accounts():
    userid = request.args['accountid']
    response = accounts_by_userid(userid)
    return response

def accounts_by_userid(userid):
    client = MongoClient("mongodb+srv://natwest_user:NatwestDxcMongo@natwestdb-x3meq.azure.mongodb.net/test")  # host uri
    useraccountdb = client.natwest_accounts
    userprofilecollection = useraccountdb.rbs_accounts_atlas

    # Transaction = []

    t = userprofilecollection.aggregate([
        {'$unwind': '$AccountData.Data.Account'},
        {'$match': {
            'AccountData.Data.Account.AccountId': userid,
        }},
        {'$project': {
            'AccountData.Data.Account': 1, '_id': 0
        }

        },
    ])

    Transaction = list(t)

    Totaltransaction = []

    for i in Transaction:
        Totaltransaction.append(i["AccountData"]["Data"]["Account"])

    AccountId = []
    AccountType = []
    AccountSubType = []
    Nickname = []
    for i in range(len(Totaltransaction)):
        AccountId.append(Totaltransaction[i]['AccountId'])
        AccountType.append(Totaltransaction[i]['AccountType'])
        AccountSubType.append(Totaltransaction[i]['AccountSubType'])
        Nickname.append(Totaltransaction[i]['Nickname'])
        df_Totaltransaction = pd.DataFrame(
            {'AccountId': AccountId, 'AccountType': AccountType, 'AccountSubType': AccountSubType,
             'Nickname': Nickname}, columns=['AccountId', 'AccountType', 'AccountSubType', 'Nickname'])

    b = df_Totaltransaction.to_json()
    return b

@app.route('/categorization', methods=['GET', 'POST'])
def categorization():
    userid = request.args['accountid']
    c = usercategorizationcollection.aggregate([
        {'$unwind': '$category'},
        {'$match': {'category.AccountId': userid}},
        {'$project': {'_id': 0, "category.Other expenses": 1, "category.Household and services": 1,
                      "category.Insurance and Fees": 1, "category.Mortgage and Interest": 1}
         },
    ])
    category = list(c)
    a = category[0]
    b = a['category']
    return b

@app.route('/logout')
def logout():
    if 'user' in session:
        session.pop('user',None)
    return redirect(url_for('home'))

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
