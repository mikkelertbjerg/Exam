import py2neo as neo
import pandas as pd
import mysql.connector

# Connection to  our Graph (Hosted by Google Cloud Platform)
graph = neo.Graph("bolt://35-202-37-187.gcp-neo4j-sandbox.com:7687", auth=("neo4j", "cy3yxxzcXDN6UKnw"), secure=True)
customerInfoDF = graph.run("MATCH (co:Country)<-[:`IS IN`]-(z:Zipcode)<-[:`IS IN`]-(ci:City)<-[:`IS IN`]-(a:Address)<-[:`LIVES AT`]-(c:Customer)-[:PLACED]->(o:Order) WHERE o.status = 'submitted' OR o.status = 'pending' RETURN c.first_name,c.last_name,c.birth_date, c.email, c.gender, c.phone, o.date, o.total, o.status").to_data_frame()

productInfoDF = graph.run("MATCH (c:Customer)-[:PLACED]->(o:Order)-[ol:CONTAINS]->(p:Product)-[:`IS A`]->(ca:Category) RETURN p.name, ol.price, ol.quantity, ca.name").to_data_frame()


def postNewOrder(customer_id):
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    orderQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.order (fk_customer_id, total, date, status) VALUES ({customer_id},{customerInfoDF['o.total'][0]}, '{customerInfoDF['o.date'][0]}:00', '{customerInfoDF['o.status'][0]}');"
    cursor.execute(orderQuery)
    connection.commit()
    order_id = cursor.lastrowid
    cursor.close()
    connection.close()

    return order_id


def postNewCustomer():
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    customerQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.customer (first_name, last_name, birth_date, gender) VALUES ('{customerInfoDF['c.first_name'][0]}','{customerInfoDF['c.last_name'][0]}', DATE '{customerInfoDF['c.birth_date'][0]}', '{customerInfoDF['c.gender'][0]}');"
    cursor.execute(customerQuery)
    connection.commit()
    customer_id = cursor.lastrowid
    cursor.close()
    connection.close()
    
    return customer_id

def postNewProduct(category_id):
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    cursor.execute(f"SELECT p.id FROM zakeovich_dk_db_cphbusiness.product p WHERE p.name = '{productInfoDF['p.name'][0]}';")
    product_id = cursor.fetchall()
    
    if product_id[0] != None:
        product_id = product_id[0]
        
        cursor.close()
        connection.close()
        return product_id[0]
    else:
        productQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.product (name, price, fk_category_id) VALUES ('{productInfoDF['p.name'][0]}', {productInfoDF['ol.price'][0]}, {category_id});"
        cursor.execute(productQuery)
        connection.commit()
        product_id = cursor.lastrowid
        
        cursor.close()
        connection.close()
        return product_id
    
    
    
   

def postNewCategory():
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    cursor.execute(f"SELECT c.id FROM zakeovich_dk_db_cphbusiness.category c WHERE c.name = '{productInfoDF['ca.name'][0]}'")
    category_id = cursor.fetchall()
    
    if category_id[0] != None:
        category_id = category_id[0]
    else:
        categoryQuery = f"INSERT INTO zakeovich_dk_db_cphbusiness.category (name) VALUES ('{productInfoDF['ca.name'][0]}');"
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
    

customer_id = postNewCustomer()
order_id = postNewOrder(customer_id)
category_id = postNewCategory()
product_id = postNewProduct(category_id)

postOrderline(order_id, product_id)