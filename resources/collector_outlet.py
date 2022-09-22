

from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_outlet import CollectorOutletModel
from models.collector_user import CollectorUserModel
from validators.errors import NotFoundError, ServerError
from validators.outlet import NewOutletSchema, OutletSchema, UpdateOutletSchema
from flask_jwt_extended import jwt_required


outletSchema = OutletSchema()
newOutletSchema = NewOutletSchema()
updateOutletSchema = UpdateOutletSchema()

class CollectorOutlet(Resource):

    @jwt_required()
    def get(self, id):
        outlet = CollectorOutletModel.find_by_id(id)
        print("Outlet:", outlet)
        if outlet:
            return outlet.json()
        return {'message': 'Outlet not found'}, 404

    @jwt_required()
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

    @jwt_required()
    def delete(self, id):
        outlet = CollectorOutletModel.find_by_id(id)
        if not outlet:
            raise NotFoundError('Outlet')
        print(outlet)
        outlet.delete()
        return {'message': 'Outlet deleted'}, 200
    

class CollectorOutletList(Resource):
    @jwt_required()
    def get(self):
        return [ outlet.json() for outlet in CollectorOutletModel.find_all() ]

    @jwt_required()
    def post(self):
        # get the list of outlets expected to be created
        
        try:
            raw_data = request.get_json()
            print(raw_data)
            if not isinstance(raw_data, list):
                raise ValidationError(["Expected a list of outlets"])

            outlets = [newOutletSchema.load(raw_outlet) for raw_outlet in raw_data]
            outlets = CollectorOutletModel.insert_many(outlets)
            return outlets, 201

        except ValidationError as err:
            print(err)
            return {"errors": err.messages}, 400
        except Exception as err:
            raise ServerError()


    @jwt_required()
    def put(self): 
        try:
            raw_data = request.get_json()
            print(raw_data)
            if not isinstance(raw_data, list):
                raise ValidationError(["Expected a list of outlets"])

            outlets = [updateOutletSchema.load(raw_outlet) for raw_outlet in raw_data]
            outlets = CollectorOutletModel.update_many(outlets)
            return outlets, 201

        except ValidationError as err:
            print(err)
            return {"errors": err.messages}, 400
        except Exception as err:
            raise ServerError()

       
class CollectorOutletListByCollector(Resource):
    @jwt_required()
    def get(self, collector_id):
        user = CollectorUserModel.find_by_id(collector_id)
        if not user or not user.area_id:
            raise NotFoundError('User')

        outlets = CollectorOutletModel.find_by_area(user.area_id)
        return [outlet.json() for outlet in outlets]

   
class CollectorOutletByArea(Resource):
    @jwt_required()
    def get(self, area_id):
        outlets = CollectorOutletModel.find_by_area(area_id)
        return [outlet.json() for outlet in outlets]