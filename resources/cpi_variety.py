
from webbrowser import get
from flask_restful import Resource, request
from marshmallow import ValidationError

from models.cpi_variety import CPIVarietyModel
from validators.substitution import SubstitutionSchema

substitutionSchema = SubstitutionSchema()

class CPIVarietyList(Resource):
    
    def get(self):
        return [ variety.json() for variety in CPIVarietyModel.get_all() ]

    def post(self):
        pass


class CPIVariety(Resource):

    def get(self, id):
        return CPIVarietyModel.find_by_id(id).json()

    def put(self):
        pass