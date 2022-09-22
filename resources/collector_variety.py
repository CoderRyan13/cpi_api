from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_variety import CollectorVarietyModel
from validators.variety import NewVarietySchema, VarietySchema
from validators.errors import NotFoundError, ServerError
from flask_jwt_extended import jwt_required, get_jwt_identity


varietySchema = VarietySchema()
newVarietySchema = NewVarietySchema()

class CollectorVarietyList(Resource):
    
    @jwt_required()
    def get(self):
        return [variety.json() for variety in CollectorVarietyModel.find_all()] 

    @jwt_required()
    def post(self):

        # get the list of outlets expected to be created        
        try:
            user_id = get_jwt_identity()
            raw_data = request.get_json()
            print(raw_data)
            if not isinstance(raw_data, list):
                raise ValidationError(["Expected a list of Varieties"])

            varieties = [newVarietySchema.load(raw_variety) for raw_variety in raw_data]
            varieties = CollectorVarietyModel.insert_many(varieties, user_id)
            return varieties, 201

        except ValidationError as err:
            print(err)
            return {"errors": err.messages}, 400
        except Exception as err:
            print(err)
            raise ServerError()
        
        
class CollectorVariety(Resource):

    @jwt_required()
    def get(self, id):
        variety = CollectorVarietyModel.find_by_id(id)
        if variety:
            return variety.json()
        raise NotFoundError("Variety")


class CollectorVarietyListByCollector(Resource):

        @jwt_required()
        def get(self, collector_id):
            print(collector_id)
            varieties = CollectorVarietyModel.find_by_collector(collector_id)
            return varieties

class CollectorVarietyListByProductId(Resource): 
    
        @jwt_required()
        def get(self, product_id):
            print(product_id)
            varieties = CollectorVarietyModel.find_by_product(product_id)
            return [variety.json() for variety in varieties]