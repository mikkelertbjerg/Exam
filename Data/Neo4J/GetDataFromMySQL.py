#GetDataFromMySQL

import mysql.connector
import py2neo as neo
from py2neo import Node, Relationship, Graph, NodeMatcher

    
def generateInvoice(order_id):
    seller = 'Team Team'
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    
    #Update order status
    cursor.execute(f"UPDATE zakeovich_dk_db_cphbusiness.order o SET o.status = 'completed' WHERE o.id = {order_id};")
    
    #Get order row
    cursor.execute(f"SELECT o.total, o.order_no, o.status FROM zakeovich_dk_db_cphbusiness.order o WHERE o.id = {order_id};")
    order_total = cursor.fetchall()
    order_total = order_total[0][0]
    
    #Generate invoice for given order and store in sql
    cursor.execute(f"INSERT INTO zakeovich_dk_db_cphbusiness.invoice (fk_order_id, total, due_date, issue_date, seller) VALUES ({order_id}, {order_total}, date_add(current_date(), INTERVAL 14 DAY), current_date(), '{seller}');")
    
    #Get the ordernumber of the order
    cursor.execute(f"SELECT o.order_no, o.status FROM zakeovich_dk_db_cphbusiness.order o WHERE o.id = {order_id};")
    order = cursor.fetchall()
    order_no = order[0][0]
    order_status = order[0][1]
    
    #Commit SQL update
    connection.commit()
    
    invoice_id = cursor.lastrowid
    cursor.execute(f"SELECT i.total, i.due_date, i.issue_date, i.seller FROM zakeovich_dk_db_cphbusiness.invoice i WHERE i.id = {invoice_id};")
    invoice = cursor.fetchall()
    
    cursor.close()
    connection.close()
    
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "1234"), secure=True)
    #graph = Graph(scheme='bolt',host='hobby-ppgaodfmmciegbkemkpmdcel.dbs.graphenedb.com',port=24787, user='dbexam', password='5.5mtzoqF4XiFf.mLUvQx7I7UiJ1QZV',secure=True)
    
    #Create a nodematcher
    matcher = NodeMatcher(graph)
    
    #Open the connection
    g_conn = graph.begin()
    order_node = matcher.match("Order", order_no = order_no).first()
    
    #Update order in Neo4j 
    graph.run(f"MATCH (o {{ order_no: {order_no}}}) SET o.status = '{order_status}'")
    
    #Serve invoice to Neo4j
    invoice_node = Node("Invoice",
                total = invoice[0][0],
                due_date = str(invoice[0][1]),
                seller = str(invoice[0][3]))

    g_conn.create(invoice_node)
    
    
    order_invoice = Relationship(order_node, "GENERATED", invoice_node, issue_date = str(invoice[0][2]))
    g_conn.create(order_invoice)
    

    
    
    #Commit the changes
    g_conn.commit()
    
generateInvoice(54)
    