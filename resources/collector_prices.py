from flask_restful import Resource, request
from marshmallow import INCLUDE, ValidationError
from models.collector_assignment import AssignmentModel
from models.collector_price import CollectorPriceModel
from validators.errors import ServerError, Validation_Error
from validators.prices import AssignmentPriceApprovalSchema
from flask_jwt_extended import  get_jwt_identity, jwt_required

assignmentPriceApprovalSchema = AssignmentPriceApprovalSchema()

#  Used to approve or reject a price for an Assignment
class CollectorAssignmentPrice(Resource):

    @jwt_required()
    def put(self):

        try:

            user_id = get_jwt_identity()

            prices = request.get_json(silent=True)

            if not isinstance(prices, list):
                raise ValidationError(["Missing prices Data!"])

            prices = [ assignmentPriceApprovalSchema.load(price,unknown=INCLUDE) for price in prices ]

            print("Uploading price Prices...")
            print(prices)

            # Used to save the prices that have been updated ONLY
            changedPrices = []

            # Loop through the assignments
            for item in prices:

                #verify if there is a price with this assignment id for current period
                price = CollectorPriceModel.find_by_assignment_id(item["assignment_id"])
            
                # if there is a price with this assignment id for current period, update the price
                if price:
                    price.update_status(item['status'], user_id)
                    changedPrices.append(price.json())

                # Otherwise verify if it is a substitution price we need to update the status for
                else:

                    # Get the assignment substitution if any otherwise just skip the update
                    substitution = AssignmentModel.find_assignment_substitution(item['assignment_id'])

                    # if there is a substitution price with this assignment id for current period, update the price status
                    if substitution:

                         #verify if there is a price with this assignment id for current period (SUBSTITUTION)
                        price = CollectorPriceModel.find_by_assignment_id(substitution.id)
                    
                        # if there is a price with this assignment id for current period, update the price (SUBSTITUTION)
                        if price:
                            price.update_status(item['status'], user_id)
                            changedPrices.append(price.json())

            return changedPrices, 200

        except ValidationError as err:
            print(err)
            raise Validation_Error()
        except Exception as err:
            print(err)
            raise ServerError()
