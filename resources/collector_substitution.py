


from flask_restful import Resource, request
from marshmallow import ValidationError
from models.collector_assignment import AssignmentModel
from models.collector_price import CollectorPriceModel

from models.collector_substitution import SubstitutionModel
from validators.substitution import SubstitutionSchema
from flask_jwt_extended import jwt_required, get_jwt_identity


substitutionSchema = SubstitutionSchema()

class SubstitutionList(Resource):

    @jwt_required()
    def get(self):
        return [ substitution.json() for substitution in SubstitutionModel.find_all() ]

    @jwt_required()
    def post(self):

        # validates the data
        try:

            user_id = get_jwt_identity()

            # loads the raw data from the body
            raw_substitutions = request.get_json()

            # validates the data
            if not isinstance(raw_substitutions, list):
                raise ValidationError({'message': "Expected a list of Substitutions"})

            substitutions = [ substitutionSchema.load(substitution) for substitution in raw_substitutions ]

            # processed substitutes 
            processed_substitutes = []

            # saves the data
            for substitution in substitutions:
                substitution = AssignmentModel.save_substitution(substitution, user_id)
                processed_substitutes.append(substitution.json())

            #respond to the client request    
            return processed_substitutes, 201

        except ValidationError as err:

            print("Error", err)
            return  {'errors': err.messages}, 400
        
        except Exception as err:
            print(err)
            return {"message": err.args}, 500
      
      


  
