from pymongo import MongoClient
import requests as rq
import time

client = MongoClient("mongodb://localhost:27017/")
db = client["store"]
col_products = db["products"]
col_departments = db["departments"]
col_store_users = db["store_users"]
col_employees_archive = db["employees_archive"]


start_time = time.time()
print(start_time)
result = db.products.find(
    {"STOCK.QUANTITY": {"$lt": 10}},
    {"_id": 0, "PRODUCT_NAME": 1, "CATEGORY.CATEGORY_NAME": 1}
    )
end_time = time.time()
print("########################################### QUERY 1 ###########################################")
for item in result:
    print("Nome do Produto:", item["PRODUCT_NAME"])
    print("Categoria:", item["CATEGORY"]["CATEGORY_NAME"])
    print("----------------------")
print("Tempo de execução da query 1:", (end_time - start_time) * 1000, "ms")


for _ in range(4):
    print('\n')



start_time = time.time()
result = db.store_users.find(
    {"FIRST_NAME": "Evonne", "LAST_NAME": "Warin"},
    {"_id": 0, "SHOPPING_SESSIONS.SESSION_ID": 1, "SHOPPING_SESSIONS.USER_ID": 1, "SHOPPING_SESSIONS.CREATED_AT": 1, "SHOPPING_SESSIONS.MODIFIED_AT": 1}
)
end_time = time.time()
print("########################################### QUERY 2 ###########################################")
for item in result:
    for session in item["SHOPPING_SESSIONS"]:
        print("SESSION_ID:", session["SESSION_ID"])
        print("USER_ID:", session["USER_ID"])
        print("CREATED_AT:", session["CREATED_AT"])
        print("MODIFIED_AT:", session["MODIFIED_AT"])
        print("----------------------")
print("Tempo de execução da query 2:", (end_time - start_time) * 1000, "ms")


for _ in range(4):
    print('\n')

start_time = time.time()
result = db.store_users.aggregate([
    {
    "$match": {
        "$expr": {
            "$gte": [
                {"$dateFromString": {"dateString": {"$arrayElemAt": ["$ORDERS.CREATED_AT", 0]}, "format": "%Y-%m-%d %H:%M:%S"}},
                {"$dateFromString": {"dateString": "2022-03-01T00:00:00Z"}}
                ]
            }
        }
    },
    {"$lookup": {
        "from": "products",
        "localField": "ORDERS.ORDER_ITEMS.PRODUCT_ID",
        "foreignField": "_id",
        "as": "PRODUCTS"
    }},
    {"$match": {"PRODUCTS.CATEGORY.CATEGORY_NAME": "Monitors"}},
    {"$project": {
        "_id": 0,
        "USERNAME": 1,
        "PHONE_NUMBER": 1,
        "EMAIL": 1
    }}
])
end_time = time.time()
print("########################################### QUERY 3 ###########################################")
for item in result:
    print("Username:", item["USERNAME"])
    print("Phone Number:", item["PHONE_NUMBER"])
    print("Email:", item["EMAIL"])
    print("----------------------")
print("Tempo de execução da query 3:", (end_time - start_time) * 1000, "ms")

for _ in range(4):
    print('\n')



start_time = time.time()
result = db.departments.aggregate([
    {"$unwind": "$EMPLOYEES"},
    {"$group": {
        "_id": "$DEPARTMENT_NAME",
        "averageSalary": {"$avg": "$EMPLOYEES.SALARY"}
        }},
    {"$project": {
        "DEPARTMENT_NAME": "$_id",
        "averageSalary": 1,
        "_id": 0
    }}
])
end_time = time.time()
print("########################################### QUERY 4 ###########################################")
for item in result:
    print("Departamento:", item["DEPARTMENT_NAME"])
    print("Salário Médio:", item["averageSalary"])
    print("----------------------")
print("Tempo de execução da query 4:", (end_time - start_time) * 1000, "ms")
