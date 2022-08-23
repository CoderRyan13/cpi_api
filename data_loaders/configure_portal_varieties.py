

import os
from flask_restful import Resource
from numpy import NaN, column_stack
import pandas as pd
from validators.errors import ServerError
from db import portal_db, portal_db_connection

UPLOAD_PATH = "imports"
REQUIRED_COLUMNS = ["SIMA_CODE_LEVEL_8", "IS_MONTHLY", "IS_HEADQUARTER"]


class ConfigurePortalVarieties(Resource):

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

# check the datatype of the columns{'IS_MONTHLY':'Int32', 'IS_HEADQUARTER':'Int32'}
def check_data_type(data: pd.DataFrame):

    try:
        # Change data  types of the columns
        data.IS_MONTHLY = data.IS_MONTHLY.astype(int)
        data.IS_HEADQUARTER = data.IS_HEADQUARTER.astype(int)

        return { 
            "error": False, 
            "data": data
        }

    except Exception as e:
        return {
            "error": True,
            "message": "Invalid data type: 'IS_MONTHLY', 'IS_HEADQUARTER' must be (0 or 1)"
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

    # query the database to verify if the variety exists
    variety_query = """SELECT id from collector_variety where code = %s"""

    for record in data.itertuples():

        portal_db.execute(variety_query, (record.SIMA_CODE_LEVEL_8,))
        variety = portal_db.fetchone()

        if variety is None:
            return {
                "error": True,
                "message": f"Invalid SIMA_CODE_LEVEL_8: {record.SIMA_CODE_LEVEL_8}, record does not exist in the database!"
            }

    return { "error": False }

# makes changes to the database
def process_varieties(data: pd.DataFrame):

    for record in data.itertuples():

        SIMA_CODE_LEVEL_8 = record.SIMA_CODE_LEVEL_8
        IS_MONTHLY = 1 if record.IS_MONTHLY > 0 else 0
        IS_HEADQUARTER =  1 if record.IS_HEADQUARTER > 0 else 0

        update_query = """ UPDATE collector_variety SET 
                                is_monthly = %s,
                                is_headquarter = %s
                            WHERE id in (SELECT id FROM (SELECT id FROM collector_variety WHERE code = %s) AS t)"""

        values = (
            IS_MONTHLY,
            IS_HEADQUARTER, 
            SIMA_CODE_LEVEL_8
        )

        portal_db.execute(update_query, values)

    portal_db_connection.commit()
        


   