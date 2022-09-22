from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_variety import CollectorVarietyModel
from validators.variety import VarietySchema
from validators.errors import NotFoundError, ServerError
from flask_jwt_extended import jwt_required

varietySchema = VarietySchema()

class VarietyList(Resource):
    
    @jwt_required()
    def get(self):
        return [variety.json() for variety in CollectorVarietyModel.find_all()] 

    @jwt_required()
    def post(self):

        try:

            raw_data = request.get_json(silent=True)
            varieties = [ varietySchema.load(variety) for variety in raw_data]
            
            for variety in varieties:
                variety.save_to_db()

            return { "message": "Varieties added successfully", "varieties": [variety.json() for variety in varieties] }, 201

        except ValidationError as err:
           return {"errors": err.messages}, 400

        except Exception as err:
            raise ServerError()
        
        
class Variety(Resource):

    @jwt_required()
    def get(self, id):
        variety = CollectorVarietyModel.find_by_id(id)
        if variety:
            return variety.json()
        return NotFoundError("Variety")
