import redis
import py2neo as neo
import pandas as pd
from neo4j import GraphDatabase

uri = "bolt://hobby-ppgaodfmmciegbkemkpmdcel.dbs.graphenedb.com:24787"
user = "admin"
passw = "b.qKKcMy6JEBOH.GdWUrSDDunj7jn6E"
driver = GraphDatabase.driver(uri, auth=(user, passw))
session = driver.session()


r = redis.Redis(host='localhost', port=6379, db=0)

graph = neo.Graph("bolt://hobby-ppgaodfmmciegbkemkpmdcel.dbs.graphenedb.com:24787", auth=("admin", "b.qKKcMy6JEBOH.GdWUrSDDunj7jn6E"), secure=True)

df = graph.run("MATCH (p:Product)-[IS_A] RETURN p.Name, p.Price, p.Category,").to_data_frame()



df.fillna(value=pd.np.nan, inplace=True)


ProductDict = df.to_dict('index')

with r.pipeline() as pipe:
   for p_id, product in ProductDict.items():
       pipe.hmset(p_id,product)
   pipe.execute()
   
    







