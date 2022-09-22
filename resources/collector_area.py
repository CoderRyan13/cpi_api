from flask_restful import Resource, request
from marshmallow import ValidationError
from models.collector_area import CollectorAreaModel

from validators.area import AreaSchema
from validators.errors import NotFoundError
from flask_jwt_extended import jwt_required

areaSchema = AreaSchema()

class CollectorArea(Resource):

    @jwt_required()
    def get(self, id):
        area = CollectorAreaModel.find_by_id(id)
        if area:
            return area.json()
        raise NotFoundError("Area") 

    @jwt_required()
    def put(self, id):
        area = CollectorAreaModel.find_by_id(id)
        if not area:
            raise NotFoundError("Area")
        
        raw_data = request.get_json()
        try:
            new_area = areaSchema.load(raw_data, instance=area)
        except ValidationError as err:
            return {"errors": err.messages}, 400
        
        area.update(new_area)
        return area.json()

    @jwt_required()
    def delete(self, id): 
        area = CollectorAreaModel.find_by_id(id)
        if not area:
            raise NotFoundError("Area")
        area.delete()
        return {'message': 'Area deleted'}

class CollectorAreaList(Resource):

    @jwt_required()
    def get(self):
        return  [area.json() for area in CollectorAreaModel.find_all()]

    @jwt_required()
    def post(self):
        raw_data = request.get_json()
        try:
            area = areaSchema.load(raw_data)
        except ValidationError as err:
            return {"errors": err.messages}, 400

        area.save_to_db()
        return area.json(), 201


    