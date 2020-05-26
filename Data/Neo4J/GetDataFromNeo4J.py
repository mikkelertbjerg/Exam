import py2neo as neo
import pandas as pd



graph = neo.Graph("bolt://35-202-37-187.gcp-neo4j-sandbox.com:7687", auth=("neo4j", "cy3yxxzcXDN6UKnw"), secure=True)
df = graph.run("MATCH (o:Order), (c:Customer), (a:Address) "
               +"WHERE o.status = 'pending' OR o.status = 'submitted' "
              +"RETURN c.name, o.total, a.street, a.no").to_data_frame()