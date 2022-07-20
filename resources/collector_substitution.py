


from flask_restful import Resource, request
from marshmallow import ValidationError

from models.collector_substitution import SubstitutionModel
from validators.substitution import SubstitutionSchema

substitutionSchema = SubstitutionSchema()

class SubstitutionList(Resource):

    def get(self):
        return [ substitution.json() for substitution in SubstitutionModel.find_all() ]

    def post(self):

        # loads the raw data from the body
        raw_substitutions = request.get_json()

        # validates the data
        try:
            substitutions = [ substitutionSchema.load(substitution) for substitution in raw_substitutions ]
        except ValidationError as err:
            return {"errors": err.messages}, 400
      
        # saves the data
        for substitution in substitutions:
            substitution.save_to_db()

        #respond to the client request    
        return {
            "message": "Substitutions created", 
            "substitutions": [substitution.json() for substitution in substitutions]
        }, 201


  
