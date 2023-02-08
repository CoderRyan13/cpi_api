from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
import mysql.connector
from env import PORTAL_CONFIGS, PORTAL_CONFIG_STRING

db = SQLAlchemy()

def get_portal_db_connection():
    return mysql.connector.connect(**PORTAL_CONFIGS)


def get_sql_alchemy_db_connection():
    return create_engine(PORTAL_CONFIG_STRING)



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

