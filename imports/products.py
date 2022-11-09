

from flask import request
from flask_restful import Resource
import pandas as pd
from db import get_portal_db_connection
from imports.validation import required_columns_exist

REQUIRED_COLUMNS = ["id", "code", "description"]

class ProductsDataImport(Resource):

    def post(self):

        # Validate file Existance
        file = request.files.get("file", None) 
        if file and file.mimetype == 'text/csv':

            # Read the File
            data = pd.read_csv(file)

            # validate the excel Columns
            result = required_columns_exist(data, REQUIRED_COLUMNS)
            if result['error']:
                return result, 400


            # remove all the rows with null values (nan)
            cleaned_data = data.dropna(how='all')

            # validates the column data type
            data_type_validity = check_data_types(cleaned_data)

            if data_type_validity.get("error"):
                return data_type_validity, 400

            cleaned_data = data_type_validity.get('data')

            # verifies that all values are valid
            data_values_validity = validate_data_values(cleaned_data)

            if data_values_validity.get("error"):
                return data_values_validity, 400

            print('Importing products...')

            results = save_to_db(data)
            if results.get('error', False):
                return results, 400

        return True, 200

def check_data_types(data):
    try:

        # Change data  types of the columns
        data.id = data.id.astype(int)

        return { 
            "error": False, 
            "data": data
        }

    except Exception as e:
        return {
            "error": True,
            "message": """
                The Fields :
                -   id: Should be Integers Only!
            """
        }


def validate_data_values(data):

    for record in data.itertuples():

        if type(record.id) is not int or not record.code or not record.description:
            return {
                "error": True,
                 "message": f"""
                    Invalid Record: id : {record.id} , code: {record.code}, description: {record.description}
                    The Fields :
                    -   id: should be Integers!
                    -   code: is required!, 
                    -   description: is required !
                """
            }
        
    return {
        "error": False
    }

def save_to_db(data):

    try: 

        # create a database connection
        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        find_query = """SELECT * FROM collector_product WHERE id = %s"""
        create_query = """ INSERT INTO collector_product (id, code, description) VALUES (%s, %s, %s)"""
        update_query = """ UPDATE collector_product SET code = %s, description = %s WHERE id = %s"""

        for record in data.itertuples():

            portal_db_cursor.execute(find_query, (record.id,))
            product = portal_db_cursor.fetchone()

            if product:
                portal_db_cursor.execute(update_query, (record.code, record.description, record.id))
            else: 
                portal_db_cursor.execute(create_query, ( record.id, record.code, record.description))

        portal_db.commit()
        portal_db.close()

        return {}
        
    except Exception as err:

        return {
            "error": True,
            "message": f"There is an error with record - id :{record.id} , code: {record.code}, description: {record.description}"
        }

  
   