

from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_outlet import CollectorOutletModel
from validators.errors import NotFoundError
from validators.outlet import OutletSchema

outletSchema = OutletSchema()

class CollectorOutlet(Resource):

    def get(self, id):
        outlet = CollectorOutletModel.find_by_id(id)
        print("Outlet:", outlet)
        if outlet:
            return outlet.json()
        return {'message': 'Outlet not found'}, 404

    def put(self, id):
        old_outlet = CollectorOutletModel.find_by_id(id)
        
        if not old_outlet:
            raise NotFoundError('Outlet')

        raw_data = request.get_json()

        try:
            outlet = outletSchema.load(raw_data)
        except ValidationError as err:
            return {"errors": err.messages}, 400

        old_outlet.update(outlet)
        return old_outlet.json(), 201

    def delete(self, id):
        outlet = CollectorOutletModel.find_by_id(id)
        if not outlet:
            raise NotFoundError('Outlet')
        print(outlet)
        outlet.delete()
        return {'message': 'Outlet deleted'}, 200
    

class CollectorOutletList(Resource):
    def get(self):
        return [ outlet.json() for outlet in CollectorOutletModel.find_all() ]

    def post(self):
        raw_data = request.get_json()
        print(raw_data)
        try:
            outlet = outletSchema.load(raw_data)
        except ValidationError as err:

            return {"errors": err.messages}, 400
        outlet.save_to_db()
        return outlet.json(), 201


class CollectorOutletListByCollector(Resource):
    def get(self, collector_id):
        print(collector_id)
        outlets = CollectorOutletModel.find_by_collector(collector_id)
        return outlets

   