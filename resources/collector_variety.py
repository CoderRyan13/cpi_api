from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_variety import CollectorVarietyModel
from validators.variety import NewVarietySchema, VarietySchema
from validators.errors import NotFoundError, ServerError

varietySchema = VarietySchema()
newVarietySchema = NewVarietySchema()

class CollectorVarietyList(Resource):
    
    def get(self):
        return [variety.json() for variety in CollectorVarietyModel.find_all()] 

    def post(self):

        # get the list of outlets expected to be created        
        try:
            raw_data = request.get_json()
            print(raw_data)
            if not isinstance(raw_data, list):
                raise ValidationError(["Expected a list of Varieties"])

            varieties = [newVarietySchema.load(raw_variety) for raw_variety in raw_data]
            varieties = CollectorVarietyModel.insert_many(varieties)
            return varieties, 201

        except ValidationError as err:
            print(err)
            return {"errors": err.messages}, 400
        except Exception as err:
            print(err)
            raise ServerError()
        
        
class CollectorVariety(Resource):

    def get(self, id):
        variety = CollectorVarietyModel.find_by_id(id)
        if variety:
            return variety.json()
        raise NotFoundError("Variety")


class CollectorVarietyListByCollector(Resource):

        def get(self, collector_id):
            print(collector_id)
            varieties = CollectorVarietyModel.find_by_collector(collector_id)
            return varieties