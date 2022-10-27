
from flask_restful import Resource

from models.form import Form
from models.settings import SettingsModel
from validators.errors import ServerError
from flask_jwt_extended import jwt_required

 
class TimePeriods(Resource):

    @jwt_required()
    def get(self):
        try:
            return Form.get_time_periods()
        except Exception as e:
            raise ServerError()
   
class CurrentTimePeriod(Resource):

    @jwt_required()
    def get(self):
        try:
            time_period = SettingsModel.get_current_time_period()
            return {'current_time_period': str(time_period)}
        except Exception as e:
            raise ServerError()