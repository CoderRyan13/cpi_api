from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_variety import CollectorVarietyModel
from validators.variety import VarietySchema
from validators.errors import NotFoundError, ServerError

varietySchema = VarietySchema()

class CollectorVarietyList(Resource):
    
    def get(self):
        return [variety.json() for variety in CollectorVarietyModel.find_all()] 

    def post(self):
        try:
            raw_variety = request.get_json(silent=True)
            print(raw_variety)
            variety = varietySchema.load(raw_variety)
            variety.save_to_db()
            return variety.json() , 201

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