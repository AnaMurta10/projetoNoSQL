import cx_Oracle as db
import json
from datetime import datetime
import pandas as pd

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()

def create_products_collection(cursor):
    #cursor.execute("ALTER TABLE DISCOUNT RENAME COLUMN CREATED_AT TO D_CREATED_AT")
    
    cursor.execute("SELECT * FROM PRODUCT P JOIN PRODUCT_CATEGORIES C ON P.CATEGORY_ID=C.CATEGORY_ID \
                   LEFT JOIN DISCOUNT D ON P.DISCOUNT_ID = D.DISCOUNT_ID \
                   LEFT JOIN STOCK S ON P.PRODUCT_ID = S.PRODUCT_ID")

    results = cursor.fetchall()    
    products = []
    columns = [desc[0] for desc in cursor.description]

    for row in results:
        product = dict(zip(columns, row))  

        CATEGORY = {
            "CATEGORY_ID": product["CATEGORY_ID"],
            "CATEGORY_NAME": product["CATEGORY_NAME"]
        }
        del product["CATEGORY_ID"]
        del product["CATEGORY_NAME"]
        product["CATEGORY"] = CATEGORY

        DISCOUNT = {
            "DISCOUNT_ID": product["DISCOUNT_ID"],
            "DISCOUNT_NAME": product["DISCOUNT_NAME"],
            "DISCOUNT_DESC": product["DISCOUNT_DESC"],
            "DISCOUNT_PERCENT": product["DISCOUNT_PERCENT"],
            "IS_ACTIVE_STATUS": product["IS_ACTIVE_STATUS"],
            "D_CREATED_AT": product["D_CREATED_AT"].strftime("%Y-%m-%d %H:%M:%S") if product["MODIFIED_AT"] is not None else None,
            "MODIFIED_AT": product["MODIFIED_AT"].strftime("%Y-%m-%d %H:%M:%S") if product["MODIFIED_AT"] is not None else None
        }
        del product["DISCOUNT_ID"]
        del product["DISCOUNT_NAME"]
        del product["DISCOUNT_DESC"]
        del product["DISCOUNT_PERCENT"]
        del product["IS_ACTIVE_STATUS"]
        del product["D_CREATED_AT"]
        del product["MODIFIED_AT"]
        product["DISCOUNT"] = DISCOUNT

        STOCK = {
            "QUANTITY": product["QUANTITY"],
            "MAX_STOCK_QUANTITY": product["MAX_STOCK_QUANTITY"],
            "UNIT": product["UNIT"]
        }
        del product["QUANTITY"]
        del product["MAX_STOCK_QUANTITY"]
        del product["UNIT"]
        product["STOCK"] = STOCK

        products.append(product)

    json_data = ""
    for product in products:
        json_data += json.dumps(product, default=serialize_datetime) + "\n"

    with open("products.json", "w") as file:
        file.write(json_data)


def create_departments_collection(cursor):
    #cursor.execute("ALTER TABLE EMPLOYEES RENAME COLUMN MANAGER_ID TO E_MANAGER_ID")
    cursor.execute("SELECT * FROM EMPLOYEES E JOIN DEPARTMENTS D ON E.DEPARTMENT_ID = D.DEPARTMENT_ID")

    results = cursor.fetchall()    
    departments = []
    columns = [desc[0] for desc in cursor.description]

    for row in results:
        department = dict(zip(columns, row))
        department_id = department["DEPARTMENT_ID"]
        department_name = department["DEPARTMENT_NAME"] 
        department_desc = department["DEPARTMENT_DESC"]
        manager_id = department["MANAGER_ID"]

        employee = {
            "EMPLOYEE_ID": department["EMPLOYEE_ID"],
            "FIRST_NAME": department["FIRST_NAME"],
            "MIDDLE_NAME": department["MIDDLE_NAME"],
            "LAST_NAME": department["LAST_NAME"],
            "DATE_OF_BIRTH": department["DATE_OF_BIRTH"].strftime("%Y-%m-%d"),
            "HIRE_DATE": department["HIRE_DATE"].strftime("%Y-%m-%d"),
            "SALARY": department["SALARY"],
            "PHONE_NUMBER": department["PHONE_NUMBER"],
            "EMAIL": department["EMAIL"],
            "SSN_NUMBER": department["SSN_NUMBER"],
            "E_MANAGER_ID": department["E_MANAGER_ID"]
        }

        department = next((dept for dept in departments if dept["DEPARTMENT_ID"] == department_id), None)

        if department is None:
            department = {
                "DEPARTMENT_ID": department_id,
                "DEPARTMENT_NAME": department_name,
                "DEPARTMENT_DESC": department_desc,
                "MANAGER_ID": manager_id,
                "EMPLOYEES": []
            }
            departments.append(department)

        department["EMPLOYEES"].append(employee)

    json_data = ""
    for department in departments:
        json_data += json.dumps(department, default=serialize_datetime) + "\n"

    with open("departments.json", "w") as file:
        file.write(json_data)


def create_employees_archive_collection(cursor):
    cursor.execute("SELECT * FROM EMPLOYEES_ARCHIVE")

    results = cursor.fetchall()    
    employees_archive = []
    columns = [desc[0] for desc in cursor.description]

    for row in results:
        employee = dict(zip(columns, row)) 

        employees_archive.append(employee)

    json_data = ""
    for employee in employees_archive:
        json_data += json.dumps(employee, default=serialize_datetime) + "\n"

    with open("employees_archive.json", "w") as file:
        file.write(json_data)

def create_shopping_session_json(cursor):
    #cursor.execute("ALTER TABLE CART_ITEM RENAME COLUMN CREATED_AT TO CT_CREATED_AT")
    #cursor.execute("ALTER TABLE CART_ITEM RENAME COLUMN MODIFIED_AT TO CT_MODIFIED_AT")
    cursor.execute("SELECT * FROM CART_ITEM CT JOIN SHOPPING_SESSION SS ON CT.SESSION_ID = SS.SESSION_ID")
    results = cursor.fetchall()    
    shopping_sessions= []
    columns = [desc[0] for desc in cursor.description]

    for row in results:
        shopping_session = dict(zip(columns, row))
        shopping_session_id = shopping_session["SESSION_ID"]
        shopping_session_user_id = shopping_session["USER_ID"] 
        shopping_session_created_at = shopping_session["CREATED_AT"].strftime("%Y-%m-%d %H:%M:%S")
        shopping_session_modified_at = shopping_session["MODIFIED_AT"].strftime("%Y-%m-%d %H:%M:%S")

        cart_item = {
            "CART_ITEM_ID": shopping_session["CART_ITEM_ID"],
            "PRODUCT_ID" : shopping_session["PRODUCT_ID"],
            "QUANTITY" : shopping_session["QUANTITY"],
            "CT_CREATED_AT": shopping_session["CT_CREATED_AT"].strftime("%Y-%m-%d %H:%M:%S"),
            "CT_MODIFIED_AT": shopping_session["CT_MODIFIED_AT"].strftime("%Y-%m-%d %H:%M:%S") if shopping_session["CT_MODIFIED_AT"] is not None else None
        }

        shopping_session = next((ss for ss in shopping_sessions if ss["SESSION_ID"] == shopping_session_id), None)

        if shopping_session is None:
            shopping_session = {
                "SESSION_ID": shopping_session_id,
                "USER_ID": shopping_session_user_id,
                "CREATED_AT": shopping_session_created_at,
                "MODIFIED_AT": shopping_session_modified_at,
                "CART_ITEMS": []
            }
            shopping_sessions.append(shopping_session)

        shopping_session["CART_ITEMS"].append(cart_item)
    
    json_data = ""
    for shopping_session in shopping_sessions:
        json_data += json.dumps(shopping_session, default=serialize_datetime) + "\n"

    with open("shopping_sessions.json", "w") as file:
        file.write(json_data)

def create_orders_json(cursor):
    #cursor.execute("ALTER TABLE ORDER_ITEMS RENAME COLUMN CREATED_AT TO OI_CREATED_AT")
    #cursor.execute("ALTER TABLE ORDER_ITEMS RENAME COLUMN MODIFIED_AT TO OI_MODIFIED_AT")
    #cursor.execute("ALTER TABLE PAYMENT_DETAILS RENAME COLUMN CREATED_AT TO PD_CREATED_AT")
    #cursor.execute("ALTER TABLE PAYMENT_DETAILS RENAME COLUMN MODIFIED_AT TO PD_MODIFIED_AT")
    cursor.execute("SELECT * FROM ORDER_DETAILS OD JOIN ORDER_ITEMS OI ON OD.ORDER_DETAILS_ID = OI.ORDER_DETAILS_ID \
                   LEFT JOIN ADDRESSES A ON OD.DELIVERY_ADRESS_ID = A.ADRESS_ID \
                   LEFT JOIN PAYMENT_DETAILS PD ON PD.ORDER_ID = OD.ORDER_DETAILS_ID")
    results = cursor.fetchall()    
    orders = []
    columns = [desc[0] for desc in cursor.description]

    for row in results:
        order = dict(zip(columns, row))
        order_details_id = order["ORDER_DETAILS_ID"]
        user_id = order["USER_ID"]
        total = order["TOTAL"]
        payment_id = order["PAYMENT_ID"]
        shipping_method = order["SHIPPING_METHOD"]
        delivery_adress_id = order["DELIVERY_ADRESS_ID"]
        created_at = order["CREATED_AT"].strftime("%Y-%m-%d %H:%M:%S")
        modified_at = order["MODIFIED_AT"].strftime("%Y-%m-%d %H:%M:%S") if order["MODIFIED_AT"] is not None else None

        adress = {
            "ADRESS_ID": order["ADRESS_ID"],
            "LINE_1": order["LINE_1"],
            "LINE_2": order["LINE_2"],
            "CITY": order["CITY"],
            "ZIP_CODE": order["ZIP_CODE"],
            "PROVINCE": order["PROVINCE"],
            "COUNTRY": order["COUNTRY"]
        }

        payment = {
            "PAYMENT_ID": order["PAYMENT_DETAILS_ID"],
            "AMOUNT": order["AMOUNT"],
            "PROVIDER": order["PROVIDER"],
            "PAYMENT_STATUS": order["PAYMENT_STATUS"],
            "PD_CREATED_AT": order["PD_CREATED_AT"].strftime("%Y-%m-%d %H:%M:%S") if order["PD_CREATED_AT"] is not None else None,
            "PD_MODIFIED_AT": order["PD_MODIFIED_AT"].strftime("%Y-%m-%d %H:%M:%S") if order["PD_MODIFIED_AT"] is not None else None
        }

        order_item = {
            "ORDER_ITEMS_ID": order["ORDER_ITEMS_ID"],
            "PRODUCT_ID" : order["PRODUCT_ID"],
            "OI_CREATED_AT": order["OI_CREATED_AT"].strftime("%Y-%m-%d %H:%M:%S") if order["OI_CREATED_AT"] is not None else None,
            "OI_MODIFIED_AT": order["OI_MODIFIED_AT"].strftime("%Y-%m-%d %H:%M:%S") if order["OI_MODIFIED_AT"] is not None else None
        }

        order = next((o for o in orders if o["ORDER_DETAILS_ID"] == order_details_id), None)
        
        if order is None:
            order = {
                "ORDER_DETAILS_ID": order_details_id,
                "USER_ID": user_id,
                "TOTAL": total,
                "PAYMENT": payment,
                "SHIPPING_METHOD": shipping_method,
                "DELIVERY_ADRESS": adress,
                "CREATED_AT": created_at,
                "MODIFIED_AT": modified_at,
                "ORDER_ITEMS": []
            }
            orders.append(order)

        order["ORDER_ITEMS"].append(order_item)
    
    json_data = ""
    for order in orders:
        json_data += json.dumps(order, default=serialize_datetime) + "\n"

    with open("orders.json", "w") as file:
        file.write(json_data)


def create_store_users_collection(cursor):
    create_shopping_session_json(cursor)
    create_orders_json(cursor)

    cursor.execute("SELECT * FROM STORE_USERS")

    results = cursor.fetchall()    
    store_users = []
    columns = [desc[0] for desc in cursor.description]

    shopping_sessions = []
    with open('shopping_sessions.json', 'r') as file2:
        for line in file2:
            json_obj = json.loads(line)
            shopping_sessions.append(json_obj)
    
    orders = []
    with open('orders.json', 'r') as file2:
        for line in file2:
            json_obj = json.loads(line)
            orders.append(json_obj)

    for row in results:
        user = dict(zip(columns, row)) 
        user_data = {
            "USER_ID": user["USER_ID"],
            "FIRST_NAME": user["FIRST_NAME"],
            "MIDDLE_NAME": user["MIDDLE_NAME"],
            "LAST_NAME": user["LAST_NAME"],
            "PHONE_NUMBER": user["PHONE_NUMBER"],
            "EMAIL": user["EMAIL"],
            "USERNAME": user["USERNAME"],
            "USER_PASSWORD": user["USER_PASSWORD"],
            "REGISTERED_AT": user["REGISTERED_AT"],
            "SHOPPING_SESSIONS": [],
            "ORDERS": []
        }

        for session in shopping_sessions:
            if user['USER_ID'] == session['USER_ID']:
                user_data["SHOPPING_SESSIONS"].append(session)

        for order in orders:
            if user['USER_ID'] == order['USER_ID']:
                user_data["ORDERS"].append(order)

        store_users.append(user_data)

    json_data = ""
    for user in store_users:
        json_data += json.dumps(user, default=serialize_datetime) + "\n"

    with open("store_users.json", "w") as file:
        file.write(json_data)


if __name__ == '__main__':
    db.init_oracle_client(lib_dir=r"C:\instantclient_21_9")
    conn = db.connect("loja","store",dsn="localhost:1521/xe")
    cursor = conn.cursor()

    create_products_collection(cursor)
    create_departments_collection(cursor)
    create_employees_archive_collection(cursor)
    create_store_users_collection(cursor)
    
    cursor.close()
    conn.close()
