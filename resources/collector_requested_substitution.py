from flask_restful import Resource, request
from marshmallow import INCLUDE, ValidationError
from models.collector_requested_substitution import RequestedSubstitutionModel
from models.settings import can_access_assignments
from validators.errors import NotFoundError, ServerError, Validation_Error
from validators.requested_substitution import RequestSubstitutionSchema, RequestedSubstitutionApprovalSchema
from flask_jwt_extended import  get_jwt_identity, jwt_required

requestedSubstitutionSchema = RequestSubstitutionSchema()
requestedSubstitutionApprovalSchema = RequestedSubstitutionApprovalSchema()

class CollectorRequestSubstitution(Resource):

    @jwt_required()
    @can_access_assignments
    def get(self):
        
        try:
            requested_substitutions = RequestedSubstitutionModel.find_all()
            return [item.json() for item in requested_substitutions], 200
        except Exception as err:
            print(err)
            raise ServerError()

    @jwt_required()
    @can_access_assignments
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
            raise Validation_Error(err)
        except Exception as err:
            print(err)
            raise ServerError()



    @jwt_required()
    @can_access_assignments
    def put(self):

        try:

            user_id = get_jwt_identity()

            assignments = request.get_json(silent=True)

            if not isinstance(assignments, list):
                raise ValidationError(["Missing assignments Data!"])

            assignments = [ requestedSubstitutionApprovalSchema.load(assignment,unknown=INCLUDE) for assignment in assignments ]

            print("Uploading price assignments...")
            print(assignments)


            # Used to save the assignments that have been updated ONLY
            changedAssignments = []

            # Loop through the assignments
            for item in assignments:

                #verify if there is a request with this assignment id for current period
                requested_substitution = RequestedSubstitutionModel.find_by_assignment_id(item["assignment_id"])
            
                # if there is a request with this assignment id for current period, update the request
                if requested_substitution and requested_substitution.status != "approved":

                    requested_substitution.update_status(item['status'])
                    changedAssignments.append(requested_substitution.json())


            return changedAssignments, 200
            

        except ValidationError as err:
            print(err)
            raise Validation_Error()
        except Exception as err:
            print(err)
            raise ServerError()

    