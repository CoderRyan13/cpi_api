

from flask import request
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restful import Resource
from numpy import product
import pandas as pd
from db import get_portal_db_connection
from imports.validation import required_columns_exist

REQUIRED_COLUMNS = ["id", "name", "code", "product_id"]

class VarietiesDataImport(Resource):

    @jwt_required()
    def post(self):

        # get user from token
        user_id = get_jwt_identity()

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

            results = save_to_db(data, user_id)
            if results.get('error', False):
                return results, 400

        return True, 200

def check_data_types(data):
    try:

        # Change data  types of the columns
        data.id = data.id.astype(int)
        data.product_id = data.product_id.astype(int)

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
                -   product_id: Should be Integers Only!
            """
        }


def validate_data_values(data):

    # Verify The Code and the Product Id 
    portal_db = get_portal_db_connection()
    portal_db_cursor = portal_db.cursor()

    find_query = """SELECT code FROM collector_product WHERE id = %s"""

    for record in data.itertuples():

        if type(record.id) is not int or not record.code or not record.name or not record.product_id:
            portal_db.close()
            return {
                "error": True,
                 "message": f"""
                    Invalid Record: id : {record.id} , code: {record.code}, description: {record.name}
                    The Fields :
                    -   id: should be Integers!
                    -   code: is required!, 
                    -   name: is required !
                """
            }
        
        # Verify if the product Exist
        portal_db_cursor.execute(find_query, (record.product_id,))
        product = portal_db_cursor.fetchone()

        if not product:
            return {
                "error": True,
                 "message": f"""
                    The Product Id {record.product_id} does not exist!
                """
            }  

        if record.code[0:23] != product[0]:
                return {
                "error": True,
                 "message": f"""
                    The Product Id {record.product_id} Cannot Support this Variety Code : {record.code}!
                """
            }      

    portal_db.close()
        
    return {
        "error": False
    }

def save_to_db(data, user_id):

    try: 

        # create a database connection
        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        find_by_id_query = """SELECT * FROM collector_variety WHERE id = %s"""
        find_by_code_query = """SELECT id FROM collector_variety WHERE code = %s"""

        create_query = """ INSERT INTO collector_variety (id, code, name, product_id, created_by) VALUES (%s, %s, %s, %s, %s)"""
        update_query = """ UPDATE collector_variety SET code = %s, name = %s, product_id = %s WHERE id = %s"""

        for record in data.itertuples():

            portal_db_cursor.execute(find_by_id_query, (record.id,))
            product = portal_db_cursor.fetchone()

            portal_db_cursor.execute(find_by_code_query, (record.code,))
            by_code_product = portal_db_cursor.fetchone()

            if product:
                if by_code_product and by_code_product[0] != record.id:
                    return {
                        "error": True,
                        "message": f"The Code '{record.code}' provided is already in use by variety - {by_code_product[0]} !"
                    }
                portal_db_cursor.execute(update_query, (record.code, record.name, record.product_id, record.id))
            else: 
                if by_code_product:
                    return {
                        "error": True,
                        "message": f"The Code '{record.code}' provided is already in use by variety - {by_code_product[0]} !"
                    }
                portal_db_cursor.execute(create_query, ( record.id, record.code, record.name, record.product_id, user_id))

        portal_db.commit()
        portal_db.close()

        return {}
        
    except Exception as err:

        print(err)

        return {
            "error": True,
            "message": f"There is an error with record - id :{record.id} , code: {record.code}, name: {record.name}, product_id: {record.product_id}"
        }

  
   