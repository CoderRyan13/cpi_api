import os
from flask_restful import Resource
from numpy import NaN, column_stack
import pandas as pd
from validators.errors import ServerError
from db import get_portal_db_connection


UPLOAD_PATH = "imports"
REQUIRED_COLUMNS = ["variety_id", "outlet_id", "is_monthly", "is_headquarter", "from_outlet_id"]


class ConfigureAssignments(Resource):

    def get(self, filename):
        try:
            # read the file
            file_path = os.path.join(UPLOAD_PATH, filename)
            
            data = pd.read_csv(file_path)

            column_validity = required_columns_exist(data)

            # if the columns are not valid, return an error
            if column_validity.get("error"):
                return column_validity, 400

            # remove all the rows with null values (nan)
            cleaned_data = data.dropna(how='all')


            # validates the column data type
            data_type_validity = check_data_type(cleaned_data)

            if data_type_validity.get("error"):
                return data_type_validity, 400

            cleaned_data = data_type_validity.get('data')



            # Check data validity
            data_existence_validity = check_data_existence_validity(cleaned_data)

            if data_existence_validity.get("error"):
                return data_existence_validity, 400

            print("RRRRR")


            # Process that requested changes to the Database
            process_varieties(cleaned_data)
          
            return {
                "success": True, 
                "message": "Data has been successfully configured!",
                "data": cleaned_data.to_dict(orient='records')      
            }, 201

        except Exception as e:
            print(e)
            raise ServerError()


# check if all columns exist
def required_columns_exist(data):

    result = {
        "missing_columns": [],
        "error": False,
    }

    for column in REQUIRED_COLUMNS:
        if column not in data.columns:
            result["missing_columns"].append(column)
    
    if len(result["missing_columns"]) > 0:
        result['error'] = True
        result['message'] = f"Required columns are missing: {result['missing_columns']}"    

    return result


# check the datatype of the columns{'is_monthly':'Int32', 'is_headquarter':'Int32', 'from_outlet_id' :'Int32'}
def check_data_type(data: pd.DataFrame):

    try:
        # Change data  types of the columns
        data.variety_id = data.variety_id.astype(int)
        data.outlet_id = data.outlet_id.astype(int)

         # Change data  types of the columns
        data.is_monthly = data.is_monthly.astype(int)
        data.is_headquarter = data.is_headquarter.astype(int)

        return { 
            "error": False, 
            "data": data
        }

    except Exception as e:
        return {
            "error": True,
            "message": """
                The Fields :
                -   outlet_id, variety_id: should be Integers!
                -   from_outlet_id: should be Integer Optional!, 
                -   is_monthly, 'is_headquarter: must be (0 or 1) !
            """
        }



# check if the data exists in the database 
def check_data_existence_validity(data: pd.DataFrame):

    portal_db = get_portal_db_connection()
    portal_db_cursor = portal_db.cursor()

    assignment_existence_query = """ SELECT id 
                            FROM assignment 
                            WHERE outlet_id = (SELECT id FROM collector_outlet WHERE cpi_outlet_id = %s)
                            AND variety_id = (SELECT id FROM collector_variety WHERE cpi_variety_id = %s)
                            AND status = 'active'
                        """
  
    result = { "error": False }

    for record in data.itertuples():

        print(record)

        if record.is_headquarter not in [0, 1] or record.is_monthly not in [0, 1]:
            result["error"] = True
            result["message"] = "is_monthly and is_headquarter must be (0 or 1) !"
            return result


        if record.variety_id is None:
            result["error"] = True,
            result["message"] = f"Invalid variety_id: {record.variety_id}, record does not exist in the database!"
            break

        
        if record.outlet_id is None:
            result["error"] = True,
            result["message"] = f"Invalid outlet_id: {record.outlet_id}, record does not exist in the database!"
            break 


        portal_db_cursor.execute(assignment_existence_query, (record.outlet_id, record.variety_id))
        assignment = portal_db_cursor.fetchone()



        if assignment is None:
            result["error"] = True,
            result["message"] = f"Invalid COMBINATION outlet_id, variety_id: {record.outlet_id}, {record.variety_id} - Assignment does not exist in the database!"
            break
        

        if not pd.isna(record.from_outlet_id):

            from_outlet_id = 0

            try: 
                from_outlet_id = int(record.from_outlet_id)

            except Exception as e: 
                result["error"] = True,
                result["message"] = f"Invalid from_outlet_id: {record.from_outlet_id}, record does not exist in the database!"
                break
            
            portal_db_cursor.execute(assignment_existence_query, (from_outlet_id, record.variety_id))
            from_outlet = portal_db_cursor.fetchone()

            if from_outlet is None:
                result["error"] = True,
                result["message"] = f"Invalid COMBINATION from_outlet_id, variety_id: {from_outlet_id}, {record.variety_id} - Assignment does not exist in the database.Therefore, other assignments to depend on this assignment!"
                break

    portal_db.close()

    return result

# makes changes to the database
def process_varieties(data: pd.DataFrame):

    portal_db = get_portal_db_connection()
    portal_db_cursor = portal_db.cursor()

    # loop through the records in the DataFrame
    for record in data.itertuples():

        variety_id = record.variety_id
        outlet_id = record.outlet_id
        if pd.isna(record.from_outlet_id) : 
            from_outlet_id = None 
        else: 
            from_outlet_id = int(record.from_outlet_id)
        is_headquarter = record.is_headquarter
        is_monthly = record.is_monthly
        
        update_assignment_query = """ UPDATE assignment SET 
                                        from_outlet_id =  (SELECT id FROM collector_outlet WHERE cpi_outlet_id = %s),
                                        is_monthly = %s,
                                        is_headquarter = %s
                                    WHERE outlet_id = (SELECT id FROM collector_outlet WHERE cpi_outlet_id = %s)
                                    AND variety_id = (SELECT id FROM collector_variety WHERE cpi_variety_id = %s)
                                    AND status = 'active'
        """
        portal_db_cursor.execute(update_assignment_query, (from_outlet_id, is_monthly, is_headquarter, outlet_id, variety_id))

    #commit transaction
    portal_db.commit()
    portal_db.close()
        
