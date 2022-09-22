from flask_sqlalchemy import SQLAlchemy
import mysql.connector
# from mysql.connector import Error

db = SQLAlchemy()


PORTAL_CONFIGS = {  
                    "host":'localhost',
                    "database":'cpi-collector',
                    "user":'root',
                    "password":'mysql'
                }

CPI_CONFIGS = {  
                    "host":'localhost',
                    "database":'cpi-1-test',
                    "user":'root',
                    "password":'mysql'
                }


def get_portal_db_connection():
    return mysql.connector.connect(**PORTAL_CONFIGS)

def get_cpi_db_connection():
    return mysql.connector.connect(**CPI_CONFIGS)



# cpi_test_db_connection = mysql.connector.connect(host='localhost',
#                                          database='cpi-1-test',
#                                          user='root',
#                                          password='mysql')

# cpi_test_db = cpi_test_db_connection.cursor()



# cpi_db_connection = mysql.connector.connect(host='localhost',
#                                          database='cpi-1-test',
#                                          user='root',
#                                          password='mysql')


# cpi_db = cpi_db_connection.cursor()



# portal_db_connection = mysql.connector.connect(host='localhost',
#                                          database='cpi-collector',
#                                          user='root',
#                                          password='mysql')

# portal_db = portal_db_connection.cursor()

