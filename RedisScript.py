import redis
import py2neo as neo
import pandas as pd

r = redis.Redis(host='localhost', port=6379, db=0)

graph = neo.Graph("bolt://localhost:7687", auth=("neo4j", "Skole123"))

df = graph.run("MATCH (m:Movie) RETURN m.title, m.released, m.tagline").to_data_frame()

df.fillna(value=pd.np.nan, inplace=True)
        


MoviesDict = df.to_dict('index')

with r.pipeline() as pipe:
   for m_id, movie in MoviesDict.items():
       pipe.hmset(m_id,movie)
   pipe.execute()
   
    







