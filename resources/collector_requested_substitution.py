from flask_restful import Resource, request
from marshmallow import INCLUDE, ValidationError
from models.collector_requested_substitution import RequestedSubstitutionModel
from validators.errors import NotFoundError, ServerError, Validation_Error
from validators.requested_substitution import RequestSubstitutionSchema

requestedSubstitutionSchema = RequestSubstitutionSchema()

class CollectorRequestSubstitution(Resource):

    def get(self):
        
        try:
            requested_substitutions = RequestedSubstitutionModel.find_all()
            return [item.json() for item in requested_substitutions], 200
        except Exception as err:
            print(err)
            raise ServerError()

    def post(self):

        try:

            requested_substitutions = request.get_json(silent=True)

            if not isinstance(requested_substitutions, list):
                raise ValidationError(["Missing requested substitutions Data!"])

            requested_substitutions = [ requestedSubstitutionSchema.load(requested_substitution,unknown=INCLUDE) for requested_substitution in requested_substitutions ]
            print("Uploading requests...")
            print(requested_substitutions)

            requested_substitutions_created = []

            for item in requested_substitutions:

                # check if a requested substitution already exists for this assignment if not create a new one
                requested_substitution = RequestedSubstitutionModel.find_by_assignment_id(item["assignment_id"])

                if not requested_substitution:
                    requested_assignment = RequestedSubstitutionModel(item["assignment_id"])
                    requested_assignment.save_to_db()
                    requested_substitutions_created.append(requested_assignment)
                
            return [item.json() for item in requested_substitutions_created], 201

        except ValidationError as err:
            print(err)
            raise Validation_Error()
        except Exception as err:
            print(err)
            raise ServerError()

    