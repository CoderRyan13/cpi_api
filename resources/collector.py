
from flask_restful import Resource

from models.form import Form
from validators.errors import ServerError
from flask_jwt_extended import jwt_required


class Collectors(Resource):

    @jwt_required()
    def get(self):
        try:
            return Form.get_collectors()
        except Exception as e:
            raise ServerError()
   