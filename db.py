from flask_sqlalchemy import SQLAlchemy
import mysql.connector
from mysql.connector import Error

db = SQLAlchemy()

conn = mysql.connector.connect(host='192.168.0.3',
                                         database='cpi2',
                                         user='sib_gian',
                                         password='Letmein123!')

cpi_db = conn.cursor()



conn2 = mysql.connector.connect(host='localhost',
                                         database='cpi-collector',
                                         user='root',
                                         password='mysql')

portal_db = conn2.cursor()

