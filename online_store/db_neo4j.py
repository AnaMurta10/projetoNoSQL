import pandas as pd
from neo4j import GraphDatabase

uri = "neo4j://localhost:7999"
username = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(username, password))

dt_addresses = pd.read_csv('datasets/addresses.csv', sep=';')
dt_cart_item = pd.read_csv('datasets/cart_item.csv', sep = ';')
dt_departments = pd.read_csv('datasets/departments.csv', sep = ';')
dt_discount = pd.read_csv('datasets/discount.csv', sep = ';')
dt_employees_archive = pd.read_csv('datasets/employees_archive.csv', sep = ';')
dt_employees = pd.read_csv('datasets/employees.csv', sep = ';')
dt_order_details = pd.read_csv('datasets/order_details.csv', sep = ';')
dt_order_items = pd.read_csv('datasets/order_items.csv', sep = ';')
dt_payment_details = pd.read_csv('datasets/payment_details.csv', sep = ';')
dt_product_categories = pd.read_csv('datasets/product_categories.csv', sep = ';')
dt_product = pd.read_csv('datasets/product.csv', sep = ';')
dt_shopping_session = pd.read_csv('datasets/shopping_session.csv', sep = ';')
dt_stock = pd.read_csv('datasets/stock.csv', sep = ';')
dt_store_users = pd.read_csv('datasets/store_users.csv', sep = ';')


with driver.session() as session:
    for _, row in dt_addresses.iterrows():
        query = """
            CREATE (:Addresses {adress_id: $adress_id, line_1: $line_1, line_2: $line_2, city: $city,
            zip_code: $zip_code, province: $province, country: $country})
        """
        session.run(query, adress_id=row['ADRESS_ID'], line_1=row['LINE_1'], line_2=row['LINE_2'],
                    city=row['CITY'], zip_code=row['ZIP_CODE'], province=row['PROVINCE'], country=row['COUNTRY'])

    for _, row in dt_cart_item.iterrows():
        query = """
            CREATE (:CartItem {cart_item_id: $cart_item_id, session_id: $session_id, product_id: $product_id,
            quantity: $quantity, created_at: $created_at, modified_at: $modified_at})
        """
        session.run(query, cart_item_id=row['CART_ITEM_ID'], session_id=row['SESSION_ID'],
            product_id=row['PRODUCT_ID'], quantity=row['QUANTITY'], created_at=row['CREATED_AT'],
            modified_at=row['MODIFIED_AT'])


    for _, row in dt_departments.iterrows():
        query = """
            CREATE (:Departments {department_id: $department_id, department_name: $department_name,
            manager_id: $manager_id, department_desc: $department_desc})
        """
        session.run(query, department_id=row['DEPARTMENT_ID'], department_name=row['DEPARTMENT_NAME'],
                    manager_id=row['MANAGER_ID'], department_desc=row['DEPARTMENT_DESC'])


    for _, row in dt_discount.iterrows():
        query = """
            CREATE (:Discount {discount_id: $discount_id, discount_name: $discount_name, discount_desc: $discount_desc,
            discount_percent: $discount_percent, is_active_status: $is_active_status, created_at: $created_at,
            modified_at: $modified_at})
        """
        session.run(query, discount_id=row['DISCOUNT_ID'], discount_name=row['DISCOUNT_NAME'],
            discount_desc=row['DISCOUNT_DESC'], discount_percent=row['DISCOUNT_PERCENT'],
            is_active_status=row['IS_ACTIVE_STATUS'], created_at=row['CREATED_AT'],
            modified_at=row['MODIFIED_AT'])


    for _, row in dt_employees_archive.iterrows():
        query = """
            CREATE (:EmployeesArchive {event_date: $event_date, event_type: $event_type, user_name: $user_name, 
                        old_employee_id: $old_employee_id, old_first_name: $old_first_name, 
                        old_middle_name: $old_middle_name, old_last_name: $old_last_name, 
                        old_date_of_birth: $old_date_of_birth, old_department_id: $old_department_id, 
                        old_hire_date: $old_hire_date, old_salary: $old_salary, 
                        old_phone_number: $old_phone_number, old_email: $old_email, 
                        old_ssn_number: $old_ssn_number, old_manager_id: $old_manager_id, 
                        new_employee_id: $new_employee_id, new_first_name: $new_first_name, 
                        new_middle_name: $new_middle_name, new_last_name: $new_last_name, 
                        new_date_of_birth: $new_date_of_birth, new_department_id: $new_department_id, 
                        new_hire_date: $new_hire_date, new_salary: $new_salary, 
                        new_phone_number: $new_phone_number, new_email: $new_email, 
                        new_ssn_number: $new_ssn_number, new_manager_id: $new_manager_id})
        """
        session.run(query, event_date=row['EVENT_DATE'], event_type=row['EVENT_TYPE'], user_name=row['USER_NAME'], 
            old_employee_id=row['OLD_EMPLOYEE_ID'], old_first_name=row['OLD_FIRST_NAME'], old_middle_name=row['OLD_MIDDLE_NAME'],
            old_last_name=row['OLD_LAST_NAME'], old_date_of_birth=row['OLD_DATE_OF_BIRTH'], old_department_id=row['OLD_DEPARTMENT_ID'], 
            old_hire_date=row['OLD_HIRE_DATE'], old_salary=row['OLD_SALARY'], old_phone_number=row['OLD_PHONE_NUMBER'], old_email=row['OLD_EMAIL'], 
            old_ssn_number=row['OLD_SSN_NUMBER'], old_manager_id=row['OLD_MANAGER_ID'], new_employee_id=row['NEW_EMPLOYEE_ID'], new_first_name=row['NEW_FIRST_NAME'],
            new_middle_name=row['NEW_MIDDLE_NAME'], new_last_name=row['NEW_LAST_NAME'], new_date_of_birth=row['NEW_DATE_OF_BIRTH'], new_department_id=row['NEW_DEPARTMENT_ID'], 
            new_hire_date=row['NEW_HIRE_DATE'], new_salary=row['NEW_SALARY'], new_phone_number=row['NEW_PHONE_NUMBER'], new_email=row['NEW_EMAIL'], new_ssn_number=row['NEW_SSN_NUMBER'], 
            new_manager_id=row['NEW_MANAGER_ID'])


    for _, row in dt_employees.iterrows():
        query = """
            CREATE (:Employees {employee_id: $employee_id, first_name: $first_name, middle_name: $middle_name, 
            last_name: $last_name, date_of_birth: $date_of_birth, department_id: $department_id, hire_date: $hire_date, salary: $salary, 
            phone_number: $phone_number, email: $email, ssn_number: $ssn_number, manager_id: $manager_id})
        """
        session.run(query, employee_id=row['EMPLOYEE_ID'], first_name=row['FIRST_NAME'], middle_name=row['MIDDLE_NAME'], last_name=row['LAST_NAME'], 
            date_of_birth=row['DATE_OF_BIRTH'], department_id=row['DEPARTMENT_ID'], hire_date=row['HIRE_DATE'], salary=row['SALARY'], 
            phone_number=row['PHONE_NUMBER'], email=row['EMAIL'], ssn_number=row['SSN_NUMBER'], manager_id=row['MANAGER_ID'])


    for _, row in dt_order_details.iterrows():
        query = """
            CREATE (:OrderDetails {order_details_id: $order_details_id, user_id: $user_id, total: $total,
            payment_id: $payment_id, shipping_method: $shipping_method, delivery_adress_id: $delivery_adress_id,
            created_at: $created_at, modified_at: $modified_at})
        """
        session.run(query, order_details_id=row['ORDER_DETAILS_ID'], user_id=row['USER_ID'], total=row['TOTAL'],
            payment_id=row['PAYMENT_ID'], shipping_method=row['SHIPPING_METHOD'],
            delivery_adress_id=row['DELIVERY_ADRESS_ID'], created_at=row['CREATED_AT'],
            modified_at=row['MODIFIED_AT'])

    
    for _, row in dt_order_items.iterrows():
        query = """
            CREATE (:OrderItems {order_items_id: $order_items_id, order_details_id: $order_details_id, 
            product_id: $product_id, created_at: $created_at, modified_at: $modified_at})
        """
        session.run(query, order_items_id=row['ORDER_ITEMS_ID'], order_details_id=row['ORDER_DETAILS_ID'],
            product_id=row['PRODUCT_ID'], created_at=row['CREATED_AT'], modified_at=row['MODIFIED_AT'])


    for _, row in dt_payment_details.iterrows():
        query = """
            CREATE (:PaymentDetails {payment_details_id: $payment_details_id, order_id: $order_id, 
            amount: $amount, provider: $provider, payment_status: $payment_status, created_at: $created_at,
            modified_at: $modified_at})
        """
        session.run(query, payment_details_id=row['PAYMENT_DETAILS_ID'], order_id=row['ORDER_ID'],
            amount=row['AMOUNT'], provider=row['PROVIDER'], payment_status=row['PAYMENT_STATUS'],
            created_at=row['CREATED_AT'], modified_at=row['MODIFIED_AT'])

        
    for _, row in dt_product_categories.iterrows():
        query = """
            CREATE (:ProductCategories {category_id: $category_id, category_name: $category_name})
        """
        session.run(query, category_id=row['CATEGORY_ID'], category_name=row['CATEGORY_NAME'])


    for _, row in dt_product.iterrows():
        query = """
            CREATE (:Product {product_id: $product_id, product_name: $product_name, category_id: $category_id,
            sku: $sku, price: $price, discount_id: $discount_id, created_at: $created_at, last_modified: $last_modified})
        """
        session.run(query, product_id=row['PRODUCT_ID'], product_name=row['PRODUCT_NAME'], category_id=row['CATEGORY_ID'],
            sku=row['SKU'], price=row['PRICE'], discount_id=row['DISCOUNT_ID'], created_at=row['CREATED_AT'],
            last_modified=row['LAST_MODIFIED'])

        
    for _, row in dt_shopping_session.iterrows():
        query = """
            CREATE (:ShoppingSession {session_id: $session_id, user_id: $user_id, created_at: $created_at, modified_at: $modified_at})
        """
        session.run(query, session_id=row['SESSION_ID'], user_id=row['USER_ID'], created_at=row['CREATED_AT'], modified_at=row['MODIFIED_AT'])


    for _, row in dt_stock.iterrows():
        query = """
            CREATE (:Stock {product_id: $product_id, quantity: $quantity, max_stock_quantity: $max_stock_quantity, unit: $unit})
        """
        session.run(query, product_id=row['PRODUCT_ID'], quantity=row['QUANTITY'], max_stock_quantity=row['MAX_STOCK_QUANTITY'], unit=row['UNIT'])


    for _, row in dt_store_users.iterrows():
        query = """
            CREATE (:StoreUsers {user_id: $user_id, first_name: $first_name, middle_name: $middle_name, last_name: $last_name,
            phone_number: $phone_number, email: $email, username: $username, user_password: $user_password, registered_at: $registered_at})
        """
        session.run(query, user_id=row['USER_ID'], first_name=row['FIRST_NAME'], middle_name=row['MIDDLE_NAME'], last_name=row['LAST_NAME'],
                    phone_number=row['PHONE_NUMBER'], email=row['EMAIL'], username=row['USERNAME'], user_password=row['USER_PASSWORD'],
                    registered_at=row['REGISTERED_AT'])                
    
    # criar relacionamento entre department e employees
    queryDE = """
        MATCH (d:Departments), (e:Employees)
        WHERE d.department_id = e.department_id
        CREATE (d)-[:has]->(e)
    """
    session.run(queryDE)

    # criar relacionamento entre employee e manager 
    queryEM = """
        MATCH (e1:Employees), (e2:Employees)
        WHERE e1.employee_id = e2.manager_id
        CREATE (e2)-[:manages]->(e1)
    """
    session.run(queryEM)

    # criar relacionamento entre department e manager
    queryDM = """
        MATCH (d:Departments), (e:Employees)
        WHERE d.manager_id = e.manager_id
        CREATE (d)-[:managed_by]->(e)
    """
    session.run(queryDM)

    # criar relacionamento entre product e product_categories 
    queryPC = """
        MATCH (pc:ProductCategories), (p:Product)
        WHERE pc.category_id = p.category_id
        CREATE (pc)-[:contains]->(p)
    """
    session.run(queryPC)

    # criar relacionamento entre product e stock
    queryPS = """
        MATCH (p:Product), (s:Stock)
        WHERE p.product_id = s.product_id
        CREATE (s)-[:of]->(p)
    """
    session.run(queryPS)

    # criar relacionamento entre product e discount
    queryPD = """
        MATCH (p:Product), (d:Discount)
        WHERE p.discount_id = d.discount_id
        CREATE (p)-[:has_a]->(d)
    """
    session.run(queryPD)

    # criar relacionamento entre shopping_session e cart_item
    querySC = """
        MATCH (ss:ShoppingSession), (ct:CartItem)
        WHERE ss.session_id = ct.session_id
        CREATE (ss)-[:includes]->(ct)
    """
    session.run(querySC)

    # criar relacionamento entre cart_item e product
    queryCP = """
        MATCH (ct:CartItem), (p:Product)
        WHERE ct.product_id = p.product_id
        CREATE (ct)-[:contains_a]->(p)
    """
    session.run(queryCP)

    # criar relacionamento entre shopping_session e store_users
    querySU = """
        MATCH (su:StoreUsers), (ss:ShoppingSession)
        WHERE su.user_id = ss.user_id
        CREATE (su)-[:created]->(ss)
    """
    session.run(querySU)

    # criar relacionamento entre store_users e order_details
    querySO = """
        MATCH (su:StoreUsers), (od:OrderDetails)
        WHERE su.user_id = od.user_id
        CREATE (su)-[:ordered]->(od)
    """
    session.run(querySO)

    # criar relacionamento entre adresses e order_details
    queryAO = """
        MATCH  (od:OrderDetails), (a:Addresses)
        WHERE od.delivery_adress_id = a.adress_id
        CREATE (od)-[:to]->(a)
    """
    session.run(queryAO)

    # criar relacionamento entre order_details e payment_details
    queryOP = """
        MATCH (od:OrderDetails), (pd:PaymentDetails)
        WHERE od.payment_id = pd.payment_details_id
        CREATE (od)-[:with]->(pd)
    """
    session.run(queryOP)

    # criar relacionamento entre order_details e order_items
    queryODOI = """
        MATCH (od:OrderDetails), (oi:OrderItems)
        WHERE od.order_details_id = oi.order_details_id
        CREATE (od)-[:ordered_a]->(oi)
    """
    session.run(queryODOI)

    # criar relacionamento entre order_items e product
    queryPO = """
        MATCH (oi:OrderItems), (p:Product)
        WHERE oi.product_id = p.product_id
        CREATE (oi)-[:about]->(p)
    """
    session.run(queryPO)

    # Remover chaves estrangeiras dos nodos
    queryRemove = """
        MATCH (n:PaymentDetails)
        WHERE n.order_id IS NOT NULL
        REMOVE n.order_id
    """
    session.run(queryRemove)

    queryRemove1 = """
        MATCH (n:Departments)
        WHERE n.manager_id IS NOT NULL
        REMOVE n.manager_id
    """
    session.run(queryRemove1)

    queryRemove2 = """
        MATCH (n:Employees)
        WHERE n.manager_id IS NOT NULL
        REMOVE n.manager_id
    """
    session.run(queryRemove2)

    queryRemove3 = """
        MATCH (n:Employees)
        WHERE n.departmente_id IS NOT NULL
        REMOVE n.department_id
    """
    session.run(queryRemove3)

    queryRemove4 = """
        MATCH (n:Product)
        WHERE n.category_id IS NOT NULL
        REMOVE n.category_id
    """
    session.run(queryRemove4)

    queryRemove5 = """
        MATCH (n:Product)
        WHERE n.discount_id IS NOT NULL
        REMOVE n.discount_id
    """
    session.run(queryRemove5)

    queryRemove6 = """
        MATCH (n:CartItem)
        WHERE n.session_id IS NOT NULL
        REMOVE n.session_id
    """
    session.run(queryRemove6)

    queryRemove7 = """
        MATCH (n:CartItem)
        WHERE n.product_id IS NOT NULL
        REMOVE n.product_id
    """
    session.run(queryRemove7)

    queryRemove8 = """
        MATCH (n:ShoppingSession)
        WHERE n.user_id IS NOT NULL
        REMOVE n.user_id
    """
    session.run(queryRemove8)

    queryRemove9 = """
        MATCH (n:OrderDetails)
        WHERE n.user_id IS NOT NULL
        REMOVE n.user_id
    """
    session.run(queryRemove9)

    queryRemove10 = """
        MATCH (n:OrderDetails)
        WHERE n.payment_id IS NOT NULL
        REMOVE n.payment_id
    """
    session.run(queryRemove10)

    queryRemove11 = """
        MATCH (n:OrderDetails)
        WHERE n.delivery_adress_id IS NOT NULL
        REMOVE n.delivery_adress_id
    """
    session.run(queryRemove11)

    queryRemove12 = """
        MATCH (n:OrderItems)
        WHERE n.product_id IS NOT NULL
        REMOVE n.product_id
    """
    session.run(queryRemove12)

    queryRemove13 = """
        MATCH (n:OrderItems)
        WHERE n.order_details_id IS NOT NULL
        REMOVE n.order_details_id
    """
    session.run(queryRemove13)

