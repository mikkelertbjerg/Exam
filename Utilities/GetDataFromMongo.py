# -*- coding: utf-8 -*-
import pymongo
import py2neo as neo
from py2neo import Node, Relationship, Graph

#MongoDB
# =============================================================================

#Credentials fro connecting to the Mongodb
#Local instance
client = pymongo.MongoClient("mongodb+srv://admin:Skole123@cluster0-wrdn4.mongodb.net/test")
#Define which "database" we're looking into
db = client["Exam"]
#Define which collection we're looking into
col = db["Cart"]
#Get an object from the selected collection
x = col.find_one()
x
#Neo4j
# =============================================================================
#Credentials for connecting to the Neo4j db: <db address> <username,password>
#Local
#graph = Graph("bolt://localhost:7687", auth=("neo4j", "test"))
#Hosted
graph = Graph("bolt://localhost:7687", auth=("neo4j", "1234"), secure=True)
#graph = Graph(scheme='bolt',host='hobby-ppgaodfmmciegbkemkpmdcel.dbs.graphenedb.com',port=24787, user='dbexam', password='5.5mtzoqF4XiFf.mLUvQx7I7UiJ1QZV',secure=True)
#Open the connection
g_conn = graph.begin()

#Define the nodes, and feed it with the information <x> fetches from mongo
#Customer
# =============================================================================
customer = Node("Customer",
                #_id = x['Customer']['CustomerId'],
                first_name = x['Customer']['Firstname'],
                last_name = x['Customer']['Lastname'],
                birth_date = x['Customer']['Birthdate'],
                gender = x['Customer']['Gender'],
                email = x['Customer']['Email'],
                phone = x['Customer']['Telephone'])

g_conn.create(customer)

#Address
# =============================================================================
address = Node("Address",
                   #_id = x['Customer']['Address']['Id'],
                   street = x['Customer']['Address']['StreetName'],
                   no = x['Customer']['Address']['Streetno'])

g_conn.create(address)

#City
# =============================================================================
city = Node("City",
            #_id = x['Customer']['Address']['City']['Id'],
            name = x['Customer']['Address']['City']['Name'])

g_conn.create(city)

#Zipcode
# =============================================================================
zipcode = Node("Zipcode",
               #_id = x['Customer']['Address']['City']['Zipcode']['Id'],
               code = x['Customer']['Address']['City']['Zipcode']['Code'],)
g_conn.create(zipcode)

#Country
# =============================================================================
country = Node("Country",
               #_id = x['Customer']['Address']['City']['Zipcode']['Country']['Id'],
               name = x['Customer']['Address']['City']['Zipcode']['Country']['Name'],
               code = x['Customer']['Address']['City']['Zipcode']['Country']['CountryCode'])
g_conn.create(country)

#Order
# =============================================================================
order = Node("Order",
             order_no = x['Customer']['Order']['OrderNumber'],
             status = x['Customer']['Order']['Status'],
             date = x['Customer']['Order']['DateCreated'],
             total = x['Customer']['Order']['Total'])
g_conn.create(order)

#Product(s)
# =============================================================================
for p in x['Customer']['Order']['Products']:
    
    quantity = p['Quantity']
    price = p['Price']
    
    product = Node('Product',
                 #_id = p['Id'],
                 name = p['Name'])
    g_conn.create(product)
    
    category = Node('Category',
                        name = p['Category'])
    g_conn.create(category)
    
    product_category = Relationship(product, "IS A", category)
    g_conn.create(product_category)
    
    order_products = Relationship(order, "CONTAINS", product, quantity = quantity, price = price)
    g_conn.create(order_products)
    #order_products['Quantity'] = quantity
    #order_products['Price'] = price

#Relationships
# =============================================================================
customer_address = Relationship(customer, "LIVES AT", address)
g_conn.create(customer_address)

address_city = Relationship(address, "IS IN", city)
g_conn.create(address_city)

city_zipcode = Relationship(city, "IS IN", zipcode)
g_conn.create(city_zipcode)

zipcode_country = Relationship(zipcode, "IS IN", country)
g_conn.create(zipcode_country)

customer_orders = Relationship(customer, "PLACED", order)
g_conn.create(customer_orders)

#Commit the changes
# =============================================================================
g_conn.commit()

#Verify commit, by checking if all variables have been commited
# =============================================================================
# print(f'Customer create: {g_conn.exists(customer)}')
# print(f'Customer create: {g_conn.exists(address)}')
# print(f'Customer create: {g_conn.exists(city)}')
# print(f'Customer create: {g_conn.exists(zipcode)}')
# print(f'Customer create: {g_conn.exists(country)}')
# print(f'Customer create: {g_conn.exists(order)}')
# print(f'Customer create: {g_conn.exists(product)}')
# print(f'Customer create: {g_conn.exists(category)}')
# print(f'Customer create: {g_conn.exists(product_category)}')
# print(f'Customer create: {g_conn.exists(order_products)}')
# print(f'Customer create: {g_conn.exists(customer_address)}')
# print(f'Customer create: {g_conn.exists(address_city)}')
# print(f'Customer create: {g_conn.exists(city_zipcode)}')
# print(f'Customer create: {g_conn.exists(zipcode_country)}')
# print(f'Customer create: {g_conn.exists(customer_orders)}')
