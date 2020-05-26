# -*- coding: utf-8 -*-
import pymongo
import py2neo as neo
from py2neo import Node, Relationship

#MongoDB
#-----------------------------------------------------------------------------------------

#Credentials fro connecting to the Mongodb
myclient = pymongo.MongoClient("mongodb+srv://admin:Skole123@cluster0-wrdn4.mongodb.net/test")
#Define which "database" we're looking into
mydb = myclient["Exam"]
#Define which collection we're looking into
mycol = mydb["Cart"]
#Get an object from the selected collection
x = mycol.find_one()

#Neo4j
#-----------------------------------------------------------------------------------------
#Credentials for connecting to the Neo4j db: <db address> <username,password>
graph = neo.Graph("bolt://localhost:7687", auth=("neo4j", "test"))
#Open the connection
g_conn = graph.begin()

#Define the nodes, and feed it with the information <x> fetches from mongo
#Customer
#-----------------------------------------------------------------------------------------
customer = neo.Node("Customer",
                #_id = x['Customer']['CustomerId'],
                name = x['Customer']['Name'],
                birth_date = x['Customer']['Birthdate'],
                gender = x['Customer']['Gender'],
                email = x['Customer']['Email'],
                phone = x['Customer']['Telephone'])

g_conn.create(customer)

#Address
#-----------------------------------------------------------------------------------------
address = neo.Node("Address",
                   #_id = x['Customer']['Address']['Id'],
                   street = x['Customer']['Address']['StreetName'],
                   no = x['Customer']['Address']['Streetno'])

g_conn.create(address)

#City
#-----------------------------------------------------------------------------------------
city = neo.Node("City",
            #_id = x['Customer']['Address']['City']['Id'],
            name = x['Customer']['Address']['City']['Name'])

g_conn.create(city)

#Zipcode
#-----------------------------------------------------------------------------------------
zipcode = neo.Node("Zipcode",
               #_id = x['Customer']['Address']['City']['Zipcode']['Id'],
               code = x['Customer']['Address']['City']['Zipcode']['Code'],)
g_conn.create(zipcode)

#Country
#-----------------------------------------------------------------------------------------
country = neo.Node("Country",
               #_id = x['Customer']['Address']['City']['Zipcode']['Country']['Id'],
               name = x['Customer']['Address']['City']['Zipcode']['Country']['Name'],
               code = x['Customer']['Address']['City']['Zipcode']['Country']['CountryCode'])
g_conn.create(country)

#Order
#-----------------------------------------------------------------------------------------
order = neo.Node("Order",
             #_id = ['Customer']['Order']['Id'],
             status = x['Customer']['Order']['Status'],
             date = x['Customer']['Order']['DateCreated'],
             total = x['Customer']['Order']['Total'])
g_conn.create(order)

#Product(s)
#-----------------------------------------------------------------------------------------
for p in x['Customer']['Order']['Products']:
    
    quantity = p['Quantity']
    price = p['Price']
    
    product = neo.Node('Product',
                 #_id = p['Id'],
                 name = p['Name'])
    g_conn.create(product)
    
    category = neo.Node('Category',
                        name = p['Category'])
    g_conn.create(category)
    
    product_category = neo.Relationship(product, "IS A", category)
    g_conn.create(product_category)
    
    order_products = neo.Relationship(order, "CONTAINS", product, quantity = quantity, price = price)
    g_conn.create(order_products)
    #order_products['Quantity'] = quantity
    #order_products['Price'] = price

#Relationships
#-----------------------------------------------------------------------------------------
customer_address = neo.Relationship(customer, "LIVES AT", address)
g_conn.create(customer_address)

address_city = neo.Relationship(address, "IS IN", city)
g_conn.create(address_city)

city_zipcode = neo.Relationship(city, "IS IN", zipcode)
g_conn.create(city_zipcode)

zipcode_country = neo.Relationship(zipcode, "IS IN", country)
g_conn.create(zipcode_country)

customer_orders = neo.Relationship(customer, "PLACED", order)
g_conn.create(customer_orders)

#Commit the changes
#-----------------------------------------------------------------------------------------
g_conn.commit()