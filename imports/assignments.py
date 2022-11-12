



from flask import request
from flask_restful import Resource
import pandas as pd
from db import get_portal_db_connection
from imports.validation import required_columns_exist

REQUIRED_COLUMNS = ["id", "variety_id", "outlet_id", "collector_id", "area_id", "is_monthly", "is_headquarter", "from_assignment_id"]

class AssignmentDataImport(Resource):

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


            print('Importing products...')

            results = save_to_db(cleaned_data)
            if results.get('error', False):
                return results, 400

        return True, 200

def check_data_types(data):
    try:

        # for record in data.itertuples():
        #     print(record)
        

        # # Change data  types of the columns
        data.id = data.id.astype(int)
        data.outlet_id = data.outlet_id.astype(int)
        data.variety_id = data.variety_id.astype(int)
        data.area_id = data.area_id.astype(int)
        # data.from_assignment_id = data.from_assignment_id.astype(int)

        # # Change data  types of the columns
        # data.is_monthly = data.is_monthly.astype(int)
        # data.is_headquarter = data.is_headquarter.astype(int)

        return { 
            "error": False, 
            "data": data
        }

    except Exception as e:
        return {
            "error": True,
            "message": """
                The Fields :
                - id: Should be Integers Only!
                - outlet_id: Should be Integers Only!
                - variety_id: Should be Integers Only!
                - area_id: Should be Integers Only!
                - from_assignment_id: Should be Integers Only!
                - is_monthly, 'is_headquarter: must be (0 or 1) !
            """
        }


def save_to_db(data):

    try: 

        # create a database connection
        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        find_variety_by_id = """SELECT * FROM collector_variety WHERE id = %s"""
        find_outlet_by_id = """SELECT * FROM collector_outlet WHERE id = %s"""
        find_area_by_id = """SELECT * FROM collector_area WHERE id = %s"""
        find_collector_by_id = """SELECT * FROM user WHERE id = %s"""
        find_assignment_by_id = """SELECT * FROM assignment WHERE id = %s"""

        update_assignment_query = """
            UPDATE assignment SET
                variety_id = %s,
                outlet_id = %s,
                collector_id = %s,
                area_id = %s,
                is_monthly = %s,
                is_headquarter = %s,
                from_assignment_id = %s
            WHERE id = %s
        """ 

        insert_assignment_query = """
            INSERT INTO assignment(
                variety_id,
                outlet_id,
                collector_id,
                area_id,
                is_monthly,
                is_headquarter,
                from_assignment_id,
                id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        result = {
            "error": False
        }

        for record in data.itertuples():

            # if record.is_headquarter not in [0, 1] or record.is_monthly not in [0, 1]:
            #     result["error"] = True
            #     result["message"] = "is_monthly and is_headquarter must be (0 or 1) !"
            #     return result

            if record.variety_id is None:
                result["error"] = True,
                result["message"] = f"Invalid variety_id: {record.variety_id}, record does not exist in the database!"
                break

            if record.outlet_id is None:
                result["error"] = True,
                result["message"] = f"Invalid outlet_id: {record.outlet_id}, record does not exist in the database!"
                break 

          
            # Verify that the values Exist
            portal_db_cursor.execute(find_variety_by_id, (record.variety_id,))
            variety = portal_db_cursor.fetchone()

            if not variety:
                result["error"] = True
                result["message"] = f"Invalid variety_id: {record.variety_id}, record does not exist in the database!"
                return result

            portal_db_cursor.execute(find_area_by_id, (record.area_id,))
            area = portal_db_cursor.fetchone()

            if not area:
                result["error"] = True
                result["message"] = f"Invalid area_id: {record.area_id}, record does not exist in the database!"
                return result

            portal_db_cursor.execute(find_collector_by_id, (record.collector_id,))
            collector = portal_db_cursor.fetchone()

            if not collector:
                result["error"] = True
                result["message"] = f"Invalid collector_id: {record.collector_id}, record does not exist in the database!"
                return result

            portal_db_cursor.execute(find_outlet_by_id, (record.outlet_id,))
            outlet = portal_db_cursor.fetchone()

            if not outlet:
                result["error"] = True
                result["message"] = f"Invalid outlet_id: {record.outlet_id}, record does not exist in the database!"
                return result


            from_assignment_id = None

            if not pd.isna(record.from_assignment_id):
                try: 
                    from_assignment_id = int(record.from_assignment_id)
                except ValueError as e:
                    result["error"] = True
                    result["message"] = f"Invalid from_assignment_id: {record.from_assignment_id}, Can only be int!"
                    return result


            if from_assignment_id:
                portal_db_cursor.execute(find_assignment_by_id, (from_assignment_id,))
                from_assignment = portal_db_cursor.fetchone()

                if not from_assignment:
                    result["error"] = True
                    result["message"] = f"Invalid from_assignment_id: {from_assignment_id}, record does not exist in the database!"
                    return result

            # Verify if the assignment already exists in the database
            portal_db_cursor.execute(find_assignment_by_id, ( record.id, ))
            assignment = portal_db_cursor.fetchone()


            if assignment: 
                portal_db_cursor.execute(update_assignment_query, ( 
                    record.variety_id,
                    record.outlet_id,
                    record.collector_id,
                    record.area_id,
                    1 if record.is_monthly else 0,
                    1 if record.is_headquarter else 0,
                    from_assignment_id if from_assignment_id else None,
                    record.id
                ))
            else:
                portal_db_cursor.execute(insert_assignment_query, ( 
                    record.variety_id,
                    record.outlet_id,
                    record.collector_id,
                    record.area_id,
                    1 if record.is_monthly else 0,
                    1 if record.is_headquarter else 0,
                   from_assignment_id if  from_assignment_id else None,
                    record.id
                ))
            
            print('Importing product - ', record.id )
            
            portal_db.commit()

        # portal_db.commit()
        portal_db.close()

        return {}
        
    except Exception as err:
        print(err)
        return {
            "error": True,
            "message": f"There is an error with record - id :{record.id} , variety_id: {record.variety_id}, collector_id: {record.collector_id}, outlet_id: {record.outlet_id}"
        }

  
   