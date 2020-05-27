#GetDataFromMySQL

import mysql.connector
import pandas as pd

def getAllOrders():
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    query = ("SELECT * FROM zakeovich_dk_db_cphbusiness.order;")
    cursor.execute(query)
    
    for(x) in cursor:
        print(x)
    
    cursor.close()
    connection.close()
    
def getAllProducts():
    connection = mysql.connector.connect(user='zakeovich_dk', password='wdfphg3mbker',
                                     host='mysql98.unoeuro.com',
                                     database='zakeovich_dk_db_cphbusiness')
    cursor = connection.cursor()
    query = ("SELECT * FROM zakeovich_dk_db_cphbusiness.product;")
    cursor.execute(query)
    for (x) in cursor:
        print(x)
    cursor.close()
    connection.close()
