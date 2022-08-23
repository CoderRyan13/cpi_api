


from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_substitution import SubstitutionModel
from validators.substitution import SubstitutionSchema

substitutionSchema = SubstitutionSchema()

class SubstitutionList(Resource):

    def get(self):
        return [ substitution.json() for substitution in SubstitutionModel.find_all() ]

    def post(self):

        # validates the data
        try:
            # loads the raw data from the body
            raw_substitutions = request.get_json()

            # validates the data
            if not isinstance(raw_substitutions, list):
                raise ValidationError(["Expected a list of Substitutions"])

            substitutions = [ substitutionSchema.load(substitution) for substitution in raw_substitutions ]

            # processed substitutes 
            processed_substitutes = []

            # saves the data
            for substitution in substitutions:
                substitution = SubstitutionModel.save_to_db(substitution)
                processed_substitutes.append(substitution.json())

            #respond to the client request    
            return processed_substitutes, 201

        except ValidationError as err:
            print("Error", err)
            return {"errors": err.messages}, 400
        
        except Exception as err:
            print(err)
            return {"message": err.args}, 500
      
      


  
