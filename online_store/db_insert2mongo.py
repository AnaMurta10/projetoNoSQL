# import cx_Oracle as db
import json
from pymongo import MongoClient

def create_views():
    from pymongo import MongoClient
    import json
    
    try:
        conn = MongoClient('mongodb+srv://ugu:nosql-tp@nosql-tp.uzn48je.mongodb.net/?retryWrites=true&w=majority')
        print("Connected successfully!!!")
    except:  
        print("Could not connect to MongoDB")
    
    # database
    db = conn.tp

    print(db)

    pipeline = [
        {
            '$unwind': {
                'path': '$SHOPPING_SESSIONS'
            }
        }, {
            '$project': {
                'SHOPPING_SESSIONS': 1
            }
        }, {
            '$lookup': {
                'from': 'products', 
                'localField': 'SHOPPING_SESSIONS.CART_ITEMS.PRODUCT_ID', 
                'foreignField': '_id', 
                'as': 'CART_ITEMS'
            }
        }, {
            '$addFields': {
                'USER_ID': '$SHOPPING_SESSIONS.USER_ID', 
                'SESSION_ID': '$SHOPPING_SESSIONS.SESSION_ID', 
                'CREATED_AT': '$SHOPPING_SESSIONS.CREATED_AT', 
                'MODIFIED_AT': '$SHOPPING_SESSIONS.MODIFIED_AT'
            }
        }, {
            '$project': {
                'USER_ID': 1, 
                'SESSION_ID': 1, 
                'CREATED_AT': 1, 
                'MODIFIED_AT': 1, 
                'CART_ITEMS': 1
            }
        }
    ]

    db.create_collection('vw_user_cart', viewOn='store_users', pipeline=pipeline)
    
def insert_data():
    # Read data
    departments = []
    with open('./json files/departments.json') as f:
        for line in f:
            departments.append(json.loads(line))

    employees_archive = []
    with open('./json files/employees_archive.json') as f:
        for line in f:
            employees_archive.append(json.loads(line))

    orders = []
    with open('./json files/orders.json') as f:
        for line in f:
            orders.append(json.loads(line))

    products = []
    with open('./json files/products.json') as f:
        for line in f:
            products.append(json.loads(line))

    store_users = []
    with open('./json files/store_users.json') as f:
        for line in f:
            store_users.append(json.loads(line))

    # Insert data
 
    try:
        conn = MongoClient('mongodb+srv://ugu:nosql-tp@nosql-tp.uzn48je.mongodb.net/?retryWrites=true&w=majority')
        print("Connected successfully!!!")
    except:  
        print("Could not connect to MongoDB")
    
    # database
    db = conn.tp

    for i in range(len(departments)):
        departments[i]['_id'] = departments[i]['DEPARTMENT_ID']
        del departments[i]['DEPARTMENT_ID']
    for i in range(len(orders)):
        orders[i]['_id'] = orders[i]['ORDER_DETAILS_ID']
        del orders[i]['ORDER_DETAILS_ID']
    for i in range(len(products)):
        products[i]['_id'] = products[i]['PRODUCT_ID']
        del products[i]['PRODUCT_ID']
    for i in range(len(store_users)):
        store_users[i]['_id'] = store_users[i]['USER_ID']
        del store_users[i]['USER_ID']

    # db.create_collection('vw_user_cart')

    db.departments.insert_many(departments)
    db.employees_archive.insert_many(employees_archive)
    db.orders.insert_many(orders)
    db.products.insert_many(products)
    db.store_users.insert_many(store_users)
    
    db.store_users.insert_one({ 'name': 'Jonathan test'})
    
    create_views()


if __name__ == '__main__':
    insert_data()