
from webbrowser import get
from flask_restful import Resource, request
from marshmallow import ValidationError

from models.cpi_variety import CPIVarietyModel
from validators.substitution import SubstitutionSchema
from flask_jwt_extended import jwt_required

substitutionSchema = SubstitutionSchema()

class CPIVarietyList(Resource):
    
    @jwt_required()
    def get(self):
        return [ variety.json() for variety in CPIVarietyModel.get_all() ]

    @jwt_required()
    def post(self):
        pass


class CPIVariety(Resource):

    @jwt_required()
    def get(self, id):
        return CPIVarietyModel.find_by_id(id).json()

    @jwt_required()
    def put(self):
        pass