from datetime import datetime
from functools import wraps
from db import get_portal_db_connection
from models.collector_user import CollectorUserModel
from validators.errors import ServerError
from flask_jwt_extended import  get_jwt_identity


class SettingsModel:

    # used to get the current time period from the database
    def get_current_time_period(): 

        portal_db = get_portal_db_connection()
        portal_db_cursor = portal_db.cursor()

        query = """
            SELECT settings.value FROM settings 
            WHERE settings.name = 'current_time_period' 
            LIMIT 1;
        """
        portal_db_cursor.execute(query)
        time_periods = portal_db_cursor.fetchall()
        portal_db.close()

        current_time_period = time_periods[0][0]

        return current_time_period

    # This Function is used to get the current time period from db to verify 
    # if the collector is allowed to access/change data
    def can_user_access_assignments(user_id):

        user: CollectorUserModel  = CollectorUserModel.find_by_id(user_id)
        current_time_period = SettingsModel.get_current_time_period()

        if user.type == 'HQ':
            return True

        if user.type == 'collector':
            system_time_period = datetime.today().strftime('%Y-%m-01')
            if system_time_period == current_time_period:
                return True

        return False

# This decorator is used to verify if the collector is allowed to access/change data
def can_access_assignments(f):

    @wraps(f)
    def decorator(*args, **kwargs):

        # get the user id from the token
        user_id = get_jwt_identity()

        try:
            # check if the user is allowed to access the data
            if not SettingsModel.can_user_access_assignments(user_id):
                return {'message': 'You are not allowed to access this data'}, 403
        except:
            raise ServerError();

        # Return the user information attached to the token
        return f(*args, **kwargs)

    return decorator



# This decorator is used to verify if the collector is allowed to access/change data
def can_access_assurance_assignments(f):
    
    @wraps(f)
    def decorator(*args, **kwargs):

        try:

            # get the user id from the token
            user_id = get_jwt_identity()

            user: CollectorUserModel  = CollectorUserModel.find_by_id(user_id)
            current_time_period = SettingsModel.get_current_time_period()

            system_time_period = datetime.today().strftime('%Y-%m-01')

            if user.type == 'HQ' and system_time_period == current_time_period:
                return f(*args, **kwargs)

            return {'message': 'You are not allowed to access this data'}, 403
           
        except:
            raise ServerError();

    return decorator        