
from flask_restful import Resource

from models.form import Form
from validators.errors import ServerError
from flask_jwt_extended import jwt_required


class TimePeriods(Resource):

    @jwt_required()
    def get(self):
        try:
            return Form.get_time_periods()
        except Exception as e:
            raise ServerError()
   