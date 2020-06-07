import py2neo as neo
import pandas as pd
import mysql.connector

# Connection to  our Graph (Hosted by Google Cloud Platform)
graph = neo.Graph("bolt://localhost:7687", auth=("neo4j", "1234"), secure=True)
customerInfoDF = graph.run("MATCH (co:Country)<-[:`IS IN`]-(z:Zipcode)<-[:`IS IN`]-(ci:City)<-[:`IS IN`]-(a:Address)<-[:`LIVES AT`]-(c:Customer)-[:PLACED]->(o:Order) WHERE o.status = 'submitted' OR o.status = 'pending' RETURN c.first_name,c.last_name,c.birth_date, c.email, c.gender, c.phone, o.date, o.total, o.status, o.order_no").to_data_frame()

#productInfoDF = graph.run("MATCH (c:Customer)-[:PLACED]->(o:Order)-[ol:CONTAINS]->(p:Product)-[:`IS A`]->(ca:Category) RETURN p.name, ol.price, ol.quantity, ca.name").to_data_frame()


def postNewOrder(customer_id, customer):
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    orderQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.order (fk_customer_id, order_no, total, date, status) VALUES ({customer_id}, {customer['o.order_no'][0]}, {customer['o.total'][0]}, '{customer['o.date'][0]}:00', '{customer['o.status'][0]}');"
    cursor.execute(orderQuery)
    connection.commit()
    order_id = cursor.lastrowid
    cursor.close()
    connection.close()

    return order_id


def postNewCustomer(customer):
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    customerQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.customer (first_name, last_name, birth_date, gender) VALUES ('{customer['c.first_name'][0]}','{customer['c.last_name'][0]}', DATE '{customer['c.birth_date'][0]}', '{customer['c.gender'][0]}');"
    cursor.execute(customerQuery)
    connection.commit()
    customer_id = cursor.lastrowid
    cursor.close()
    connection.close()
    
    return customer_id

def postNewProduct(category_id, product, index):
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    proName = product['ca.name'][index]
    proPrice = product['ol.price'][index]
    cursor = connection.cursor()
    cursor.execute(f"SELECT p.id FROM zakeovich_dk_db_cphbusiness.product p WHERE p.name = '{proName}';")
    product_id = cursor.fetchall()
    
    if len(product_id) != 0:
        product_id = product_id[0][0]
        
        cursor.close()
        connection.close()
        return product_id
    else:
        productQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.product (name, price, fk_category_id) VALUES ('{proName}', {proPrice}, {category_id});"
        cursor.execute(productQuery)
        connection.commit()
        product_id = cursor.lastrowid
        
        cursor.close()
        connection.close()
        return product_id
    
def postNewCategory(product, index):
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    
    p_catagory_ = product['ca.name'][index]
    cursor = connection.cursor()
    cursor.execute(f"SELECT c.id FROM zakeovich_dk_db_cphbusiness.category c WHERE c.name = '{p_catagory_}'")    
    list_category_id = cursor.fetchall()
    
    if len(list_category_id) != 0:
        category_id = list_category_id[0][0]
    else:
        categoryQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.category (name) VALUES ('{p_catagory_}');"
        cursor.execute(categoryQuery)
        connection.commit()
        category_id = cursor.lastrowid
    
    cursor.close()
    connection.close()  
    
    return category_id

def postOrderline(order_id, product_id):
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    orderlineQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.order_line (fk_order_id, fk_product_id, quantity) VALUES ({order_id}, {product_id}, {productInfoDF['ol.quantity'][0]});"
    cursor.execute(orderlineQuery)
    connection.commit()
    
    cursor.execute(f"SELECT calculate_subtotal({product_id}, {order_id})")
    cursor.fetchone()
    
    cursor.close()
    connection.close()
    
x = 0
y = 0

while x < len(customerInfoDF):
    customer = customerInfoDF.iloc[[x]]
    customer_id = postNewCustomer(customer)
    order_id = postNewOrder(customer_id, customer)
    productInfoDF = graph.run(f"MATCH (c:Customer)-[:PLACED]->(o:Order)-[ol:CONTAINS]->(p:Product)-[:`IS A`]->(ca:Category) WHERE c.email = '{customer['c.email'][0]}'  RETURN p.name, ol.price, ol.quantity, ca.name").to_data_frame()
    
    x += 1
    while y < len(productInfoDF):
        product = productInfoDF.iloc[[y]]
        category_id = postNewCategory(product, y)
        product_id = postNewProduct(category_id, product, y)
        postOrderline(order_id, product_id)
        y += 1
        

