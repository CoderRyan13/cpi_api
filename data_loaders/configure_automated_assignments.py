

import os
from flask_restful import Resource
from numpy import NaN, column_stack
import pandas as pd
from validators.errors import ServerError
from db import portal_db, portal_db_connection

UPLOAD_PATH = "imports"
REQUIRED_COLUMNS = ["SIMA_CODE_LEVEL_8", "OUTLET_ID", "FROM_OUTLET_ID"]


class ConfigureAutomatedAssignments(Resource):

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

            # verifies if a code is supplied for every row
            variety_codes_validity = validate_variety_codes(cleaned_data)

            if variety_codes_validity.get("error"):
                return variety_codes_validity, 400

            # validates the column data type
            data_type_validity = check_data_type(cleaned_data)

            if data_type_validity.get("error"):
                return data_type_validity, 400

            cleaned_data = data_type_validity.get('data')

            # Check data validity
            data_existence_validity = check_data_existence_validity(cleaned_data)

            if data_existence_validity.get("error"):
                return data_existence_validity, 400

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

# check the datatype of the columns{'IS_MONTHLY':'Int32', 'IS_HEADQUARTER':'Int32', 'FROM_OUTLET_ID' :'Int32'}
def check_data_type(data: pd.DataFrame):

    try:
        # Change data  types of the columns
        data.OUTLET_ID = data.OUTLET_ID.astype(int)
        data.FROM_OUTLET_ID = data.FROM_OUTLET_ID.astype(int)

        return { 
            "error": False, 
            "data": data
        }

    except Exception as e:
        return {
            "error": True,
            "message": "The Fields OUTLET_ID and FROM_OUTLET_ID should be Integers!"
        }

# verifies if any of the required columns is empty
def validate_variety_codes(data: pd.DataFrame):

    if(data['SIMA_CODE_LEVEL_8'].isnull().values.any()):
        return {
            "error": True,
            "message": "SIMA_CODE_LEVEL_8 values are required!"
        }

    return { "error": False }

# check if the data exists in the database 
def check_data_existence_validity(data: pd.DataFrame):

    variety_query = """SELECT id from collector_variety where code = %s"""
    outlet_query = """SELECT id from collector_outlet where cpi_outlet_id = %s"""

    result = { "error": False }

    for record in data.itertuples():

        portal_db.execute(variety_query, (record.SIMA_CODE_LEVEL_8,))
        variety = portal_db.fetchone()


        if variety is None:
            result["error"] = True,
            result["message"] = f"Invalid SIMA_CODE_LEVEL_8: {record.SIMA_CODE_LEVEL_8}, record does not exist in the database!"
            break

        portal_db.execute(outlet_query, (record.OUTLET_ID,))
        outlet = portal_db.fetchone()

        if outlet is None:
            result["error"] = True,
            result["message"] = f"Invalid OUTLET_ID: {record.OUTLET_ID}, record does not exist in the database!"
            break 

        portal_db.execute(outlet_query, (record.FROM_OUTLET_ID,))
        from_outlet = portal_db.fetchone()

        if from_outlet is None:
            result["error"] = True,
            result["message"] = f"Invalid FROM_OUTLET_ID: {record.FROM_OUTLET_ID}, record does not exist in the database!"
            break
    
    return result

# makes changes to the database
def process_varieties(data: pd.DataFrame):

    # loop through the records in the DataFrame
    for record in data.itertuples():

        SIMA_CODE_LEVEL_8 = record.SIMA_CODE_LEVEL_8
        OUTLET_ID = record.OUTLET_ID
        FROM_OUTLET_ID = record.FROM_OUTLET_ID

        # verify if the automated_assignment already exist for that code and outlet_id
        find_query = "SELECT id FROM automated_assignment WHERE outlet_id = %s AND code = %s"
        portal_db.execute(find_query, (OUTLET_ID, SIMA_CODE_LEVEL_8))
        automated_assignment = portal_db.fetchone()


        # find the outlet id from the cpi_outlet_id
        find_outlet_query = "SELECT id FROM collector_outlet WHERE cpi_outlet_id = %s"
        portal_db.execute(find_outlet_query, (OUTLET_ID,))
        outlet = portal_db.fetchone()
        OUTLET_ID = outlet[0]

        portal_db.execute(find_outlet_query, (FROM_OUTLET_ID,))
        outlet = portal_db.fetchone()
        FROM_OUTLET_ID = outlet[0]


        values = (
                FROM_OUTLET_ID, 
                SIMA_CODE_LEVEL_8,
                OUTLET_ID
            )
    
        # if the automated_assignment does not exist, create it otherwise update it.
        if automated_assignment:
            update_query = """  UPDATE automated_assignment SET 
                                    from_outlet_id = %s, 
                                    updated_at = CURRENT_TIMESTAMP 
                                WHERE code = %s AND outlet_id = %s """

            portal_db.execute(update_query, values)
        
        else:
            create_query = """INSERT INTO automated_assignment (
                                'from_outlet_id',
                                'code', 
                                'outlet_id' 
                            ) VALUES (%s, %s, %s)"""

            portal_db.execute(create_query, values)

    #commit transaction
    portal_db_connection.commit()
        


   