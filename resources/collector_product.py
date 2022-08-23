



from flask import request
from flask_restful import Resource
from marshmallow import ValidationError

from models.collector_product import CollectorProductModel
from validators.errors import NotFoundError
from validators.product import ProductSchema

productSchema = ProductSchema()
class CollectorProductList(Resource):
    def get(self):

        #get the query parameters 
        args = request.args.to_dict()

        #get the products from the database
        return [product.json() for product in CollectorProductModel.find_all(args)]

    def post(self):
        raw_data = request.get_json()
        try:
            product = productSchema.load(raw_data)
        except ValidationError as err:
            return {"errors": err.messages}, 400

        product.save_to_db()
        return product.json(), 201

class CollectorProduct(Resource):
    def get(self, id):
        product = CollectorProductModel.find_by_id(id)
        if not product:
            raise NotFoundError("Product")
        return product.json()

    def put(self, id):
        product = CollectorProductModel.find_by_id(id)
        if not product:
            raise NotFoundError("Product")
        raw_data = request.get_json()
        try:
            new_product = productSchema.load(raw_data)
        except ValidationError as err:
            return {"errors": err.messages}, 400
        
        product.update(new_product)
        return product.json()

    def delete(self, id):
        product = CollectorProductModel.find_by_id(id)
        if not product:
            raise NotFoundError("Product")
        product.delete()
        return {'message': 'Product deleted'}