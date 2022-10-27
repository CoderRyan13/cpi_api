from flask_restful import Resource, request
from marshmallow import ValidationError
from models.collector_price import CollectorPriceModel
from models.collector_user import CollectorUserModel

from models.collector_variety import CollectorVarietyModel
from models.settings import can_access_assignments
from validators.variety import NewVarietySchema, VarietySchema
from validators.errors import NotFoundError, ServerError, Validation_Error
from flask_jwt_extended import jwt_required, get_jwt_identity


varietySchema = VarietySchema()
newVarietySchema = NewVarietySchema()

class CollectorVarietyList(Resource):
    
    # @jwt_required()
    def get(self):
        try:
            # get the query string parameters
            query = request.args.to_dict()

            # get the filters ready
            filter = {
                "search": query.get("search", ''),
                "page": query.get("page", None),
                "rows_per_page": query.get("rows_per_page", None),
                "sort_by": query.get("sort_by", None),
                "sort_desc": query.get("sort_desc", False),
            }

            # get the filters validated
            try:

                if filter['page'] and filter['rows_per_page'] :

                    int(filter['page'])
                    int(filter['rows_per_page'])

            except:
                raise Validation_Error("Invalid page or rows_per_page")


            # get the list of varieties and total
            result = CollectorVarietyModel.find_all(filter)
            return {"total": result["count"], "varieties": [variety.json() for variety in result["varieties"] ] }, 200

        except Exception as e:
            print(e)
            raise ServerError()

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

            # check if user is allowed to approve
            user = CollectorUserModel.find_by_id(user_id)

            # if user is not allowed to approve, set the approved_by to the user:
            if user.type == 'HQ':
                for variety in varieties: 
                    CollectorVarietyModel.approve_variety(variety['id'], user_id)

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

    @jwt_required()
    def put(self, id):
        try:
            raw_data = request.get_json()
            
            variety = newVarietySchema.load(raw_data)
            
            variety_Obj = CollectorVarietyModel.find_by_id(id)
            
            if variety_Obj: 
                variety_Obj.update( variety )
                return variety_Obj.json(), 200

            raise NotFoundError("Variety")
        
        except ValidationError as err:
            print(err)
            return {"errors": err.messages}, 400

        except Exception as err:
            print(err)
            raise ServerError()

class CollectorVarietyListByCollector(Resource):

        @jwt_required()
        @can_access_assignments
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