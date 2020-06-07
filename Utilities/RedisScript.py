import redis
import py2neo as neo
import pandas as pd
from neo4j import GraphDatabase


r = redis.Redis(host='localhost', port=6379, db=0)

graph = neo.Graph("bolt://localhost:7687", auth=("neo4j", "Skole123"))

df = graph.run("MATCH (o:Order)-[co:CONTAINS]->(p:Product)-[i:`IS A`]->(c:Category) RETURN p.name, co.price, c.name").to_data_frame()



df.fillna(value=pd.np.nan, inplace=True)


ProductDict = df.to_dict('index')

with r.pipeline() as pipe:
   for p_id, product in ProductDict.items():
       pipe.hmset(p_id,product)
   pipe.execute()
   
    







