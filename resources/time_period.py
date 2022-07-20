
from flask_restful import Resource

from models.form import Form
from validators.errors import ServerError


class TimePeriods(Resource):

    def get(self):
        try:
            return Form.get_time_periods()
        except Exception as e:
            raise ServerError()
   