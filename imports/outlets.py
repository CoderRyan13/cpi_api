

from flask import request
from flask_restful import Resource
from numpy import nan
import pandas as pd
from db import get_portal_db_connection
from imports.validation import required_columns_exist

REQUIRED_COLUMNS = ["id", "est_name", "address", "phone", "note", "area_id", "long", "lat"] 

class OutletsDataImport(Resource):

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
        data.area_id = data.area_id.astype(int)

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
                -   area_id: Should be Integers Only!
            """
        }


def validate_data_values(data):

     # Verify The Code and the Product Id 
    portal_db = get_portal_db_connection()
    portal_db_cursor = portal_db.cursor()

    find_query = """SELECT id FROM collector_area WHERE id = %s"""

    for record in data.itertuples():

        if type(record.id) is not int or not record.est_name or not record.area_id:
            return {
                "error": True,
                 "message": f"""
                    Invalid Record: id : {record.id} , code: {record.est_name}, description: {record.area_id}
                    The Fields :
                    -   id: should be Integers!
                    -   est_name: is required!, 
                    -   area_id: is required! 
                """
            }
        
         # Verify if the product Exist
        portal_db_cursor.execute(find_query, (record.area_id,))
        area = portal_db_cursor.fetchone()

        if not area:
            return {
                "error": True,
                 "message": f"""
                    The Area Id {record.area_id} does not exist!
                """
            }

    portal_db.close()  
        
    return {
        "error": False
    }

def save_to_db(data):

    try: 

        # create a database connection
        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        find_query = """SELECT * FROM collector_outlet WHERE id = %s"""
        create_query = """ INSERT INTO collector_outlet (id, est_name, address, phone, note, area_id, collector_outlet.long, lat) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        update_query = """ UPDATE collector_outlet SET est_name = %s, address = %s, phone = %s, note = %s, area_id = %s, collector_outlet.long = %s, lat = %s WHERE id = %s"""

        for record in data.itertuples():

            portal_db_cursor.execute(find_query, (record.id,))
            product = portal_db_cursor.fetchone()

            if product:
                portal_db_cursor.execute(update_query, (
                    record.est_name, 
                    record.address if not pd.isna(record.address) else None , 
                    record.phone if not pd.isna(record.phone) else 0 , 
                    record.note if not pd.isna(record.note) else None , 
                    record.area_id if not pd.isna(record.area_id) else None , 
                    record.long if not pd.isna(record.long) else None , 
                    record.lat if not pd.isna(record.lat) else None , 
                    record.id
                ))
            else: 
                portal_db_cursor.execute(create_query, ( 
                    record.id, 
                    record.est_name, 
                    record.address if not pd.isna(record.address) else None , 
                    record.phone if not pd.isna(record.phone) else 0 , 
                    record.note if not pd.isna(record.note) else None , 
                    record.area_id if not pd.isna(record.area_id) else None , 
                    record.long if not pd.isna(record.long) else None , 
                    record.lat if not pd.isna(record.lat) else None , 
                ))

        portal_db.commit()
        portal_db.close()

        return {}
        
    except Exception as err:
        print(err)
        return {
            "error": True,
            "message": f"There is an error with record - id :{record.id} , est_name: {record.est_name}, address: {record.area_id}"
        }

  
   