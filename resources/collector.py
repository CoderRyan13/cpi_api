
from flask_restful import Resource

from models.form import Form
from validators.errors import ServerError


class Collectors(Resource):

    def get(self):
        try:
            return Form.get_collectors()
        except Exception as e:
            raise ServerError()
   