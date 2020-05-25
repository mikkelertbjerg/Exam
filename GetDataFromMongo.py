# -*- coding: utf-8 -*-

import pymongo
import py2neo as neo
from py2neo import Node, Relationship

#MongoDB
#Credentials fro connecting to the Mongodb
myclient = pymongo.MongoClient("mongodb+srv://admin:Skole123@cluster0-wrdn4.mongodb.net/test")
#Define which "database" we're looking into
mydb = myclient["Exam"]
#Define which collection we're looking into
mycol = mydb["Cart"]

x = mycol.find_one()

print(x)

#Neo4j
#Credentials for connecting to the Neo4j db: <db address> <username,password>
graph = neo.Graph("bolt://localhost:7687", auth=("neo4j", "test"))

#Open the connection
g_conn = graph.begin()

#Define the node, and feed it with the information <x> fetches from mongo
name = neo.Node("Name", name=x['name'])
#Merge the node in the db
g_conn.merge(name, primary_label="Name", primary_key=("name"))
#Commit the changes
g_conn.commit()


#SUDO CODE
#Nodes
customer = Node("Customer",
                first_name = x[''],
                last_name = x[''],
                birth_date = x[''],
                gender = x[''],
                contact_info = x['']) #Look into how we define contact info in the graph.

address = Node("Address",
               street = x[''],
               no = x[''])

city = Node("City",
            name = x[''])

zipcode = Node("Zipcode",
               code = x[''])

country = Node("Country",
               name = x[''],
               code = x[''])

#Look into what an order actually should contain, now that orderline is describe by the relation between order and product
order = Node("Order",
            status = x[''],
            date  = x[''],
            total = x['']) 

product = Node("Product",
               name = x[''],
               price = x[''])

category = Node ('Category',
                 name = x[''])

#Relationships
customer_address = Relationship(customer, "LIVES AT", address)
address_city = Relationship(address, "IS IN", city)
city_zipcode = Relationship(city, "IS IN", zipcode)
zipcode_country = Relationship(zipcode, "IS IN", country)
customer_orders = Relationship(customer, "PLACED", order)
order_products = Relationship(order, "CONTAINS", product)

#Potentially smart to make graph objects?: https://py2neo.org/v4/ogm.html#labels

