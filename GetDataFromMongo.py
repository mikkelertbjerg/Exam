# -*- coding: utf-8 -*-

import pymongo
import py2neo as neo

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
