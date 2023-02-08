

from flask import request
from flask_restful import Resource
from numpy import nan
import pandas as pd
from db import get_portal_db_connection
from imports.validation import required_columns_exist


class PricesDataImport(Resource):

    def post(self):

        time_period = '2022-10-01'

        if not time_period:
            return {
                "error": True,
                "message": 'No time period specified'
            }, 400

        # Validate file Existance
        file = request.files.get("file", None) 
        if file and file.mimetype == 'text/csv':

            # Read the File
            data = pd.read_csv(file)

            # validate the excel Columns
            # result = required_columns_exist(data, ['assignment_id'].extend([time_period]))
            # if result['error']:
            #     return result, 400


            # remove all the rows with null values (nan)
            cleaned_data = data.dropna(how='all')

            # validates the column data type
            data_type_validity = check_data_types(cleaned_data)

            if data_type_validity.get("error"):
                return data_type_validity, 400

            cleaned_data = data_type_validity.get('data')

            # # verifies that all values are valid
            # data_values_validity = validate_data_values(cleaned_data, time_periods)

            # if data_values_validity.get("error"):
            #     return data_values_validity, 400

            print('Importing products...')

            results = save_to_db(cleaned_data, time_period)
            if results.get('error', False):
                return results, 400

        return True, 200

def check_data_types(data):
    try:

        # Change data  types of the columns
        data.assignment_id = data.assignment_id.astype(int)

        return { 
            "error": False, 
            "data": data
        }

    except Exception as e:
        print(e)
        return {
            "error": True,
            "message": """
                The Fields :
                -   assignment_id: Should be Integers Only!
            """
        }


# def validate_data_values(data, time_periods):

#      # Verify The Code and the Product Id 
#     portal_db = get_portal_db_connection()
#     portal_db_cursor = portal_db.cursor()

#     find_query = """SELECT id FROM assignment WHERE id = %s"""

#     for record in data.itertuples():

#         try:
            
#             # for time_period in time_periods:
#             #     float(getattr(record, time_period))

#         except ValueError:
#             return {
#                 "error": True,
#                  "message": f"""
#                     The Assignment Id {record.assignment_id} has an invalid price!
#                 """
#             }
        
#         # Verify if the product Exist
#         portal_db_cursor.execute(find_query, (record.assignment_id,))
#         area = portal_db_cursor.fetchone()

#         if not area:
#             return {
#                 "error": True,
#                  "message": f"""
#                     The Assignment Id {record.assignment_id} does not exist!
#                 """
#             }

#     portal_db.close()  
        
#     return {
#         "error": False
#     }

def save_to_db(data, time_period):

    try: 

        # create a database connection
        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        # find_query = """SELECT id FROM price WHERE assignment_id = %s AND time_period = %s"""
        create_query = """ INSERT INTO price (assignment_id, price, comment, flag,  time_period, collected_at, collector_id, status) VALUES (%s, %s, %s, %s,  %s, %s, %s, 'approved')"""
        # update_query = """ UPDATE price SET price = %s WHERE id = %s"""

        for record in data.itertuples():

            print(record)

            # for time_period in time_periods:

                # portal_db_cursor.execute(find_query, (record.assignment_id, getattr(record, time_period)))
                # price = portal_db_cursor.fetchone()

                # if price:
                #      portal_db_cursor.execute(update_query, ( getattr(record, time_period), price[0]))

                # else: 
            portal_db_cursor.execute(create_query, ( 
                record.assignment_id, 
                record.price,
                "IMPORTED",
                'IMPUTED' if record.price == 0 else None, 
                time_period,
                time_period,
                2
            ))
                

        portal_db.commit()
        portal_db.close()

        return {}
        
    except Exception as err:
        print(err)
        return {
            "error": True,
            "message": f"There is an error with record - id :{record.assignment_id} Please verify values"
        }





