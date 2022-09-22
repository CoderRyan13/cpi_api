from tkinter import E
from flask_restful import Resource, request
from marshmallow import INCLUDE, ValidationError

from models.collector_assignment import AssignmentModel
from models.collector_price import CollectorPriceModel
from validators.assignment import AssignmentIdentitySchema, AssignmentPricesIdentitySchema, AssignmentSchema
from validators.errors import NotFoundError, ServerError, Validation_Error
from flask_jwt_extended import jwt_required, get_jwt_identity

assignmentSchema = AssignmentSchema()
assignmentIdentitySchema = AssignmentIdentitySchema()
assignmentPricesIdentitySchema = AssignmentPricesIdentitySchema()

class CollectorAssignmentList(Resource):

    @jwt_required()
    def get(self):
        try:
            args = request.args.to_dict()
            return AssignmentModel.find_assignments(args)
        except Exception as e:
            print(e)
            raise ServerError()

    @jwt_required()

    def post(self):
        # loads the raw data from the body
        raw_assignments = request.get_json()

        # validates the data
        try:
            assignments = [ assignmentSchema.load(assignment) for assignment in raw_assignments ]
        except ValidationError as err:
            print(err)
            return {"errors": err.messages}, 400

        # saves the data
        for assignment in assignments:
            assignment.save_to_db()

        #respond to the client request    
        return {
            "message": "Assignment Submitted Successfully", 
            "assignments": [ assignment.json() for assignment in assignments]
        }, 201
class CollectorAssignment(Resource):

    # @jwt_required()
    def get(self, id):
        assignment = AssignmentModel.find_by_id(id)
        if assignment:
            return assignment.json()
        return NotFoundError("Assignment")          
class ActivateAssignments(Resource):
    
    @jwt_required()
    def put(self, status):

        if status not in ["active", "inactive", "rejected"]:
            return {"message": "Invalid Status"}, 400

        assignments = request.get_json(silent=True)
        try:
            assignments = [ assignmentIdentitySchema.load(assignment,unknown=INCLUDE) for assignment in assignments ]
        except ValidationError as err:
            print(err)
            raise Validation_Error()

        changeAssignments = []

        for assignment in assignments:
            assignment = AssignmentModel.find_by_id(assignment["id"])
            if assignment:
                assignment.update_status(status)
                changeAssignments.append(assignment.json())
            
        return changeAssignments, 200
class UploadAssignmentsPrices(Resource):

    @jwt_required()
    def put(self):

        try:

            user_id= get_jwt_identity()

            assignments = request.get_json(silent=True)

            if not isinstance(assignments, list):
                raise ValidationError(["Missing assignments Data!"])

            assignments = [ assignmentPricesIdentitySchema.load(assignment,unknown=INCLUDE) for assignment in assignments ]
            print("Uploading assignment Prices...")

            changeAssignmentPrices = []

            for item in assignments:

                #verify if there is a price with this assignment id
                price = CollectorPriceModel.find_by_assignment_id(item["id"])

                if not price:

                    new_price = CollectorPriceModel.create_assignment_price({
                        "assignment_id": item["id"],
                        "price": item["new_price"],
                        "collected_at": item["collected_at"],
                        "comment": item["comment"],
                        "collector_id": user_id
                    })
                    changeAssignmentPrices.append(new_price.json())

                #if approved or inactive the assignment cannot be changed
                elif price:
                    if price.status not in ["approved"]:
                        price.update_price(item["new_price"], item["collected_at"], item["comment"], user_id)
                        changeAssignmentPrices.append(price.json())
                
                # clear assignment price for substitution if any
                AssignmentModel.clear_assignment_substitution(item["id"])
                
            return [assignmentPrice for assignmentPrice in changeAssignmentPrices], 200

        except ValidationError as err:
            print(err)
            raise Validation_Error()
        except Exception as err:
            print(err)
            raise ServerError()
class CollectorAssignmentListByCollector(Resource):
    @jwt_required()
    def get(self, collector_id):
        try:
            return AssignmentModel.find_by_collector(collector_id) 
        except Exception as err:
            print(err)
            raise ServerError()
            
class CollectorAutomatedAssignmentList(Resource):
    @jwt_required()
    def get(self):
        try:
            return AssignmentModel.find_automated_assignments() 
        except Exception as err:
            print(err)
            raise ServerError()

class CollectorHeadquarterAssignmentList(Resource):
    @jwt_required()
    def get(self):
        try:
            return AssignmentModel.find_headquarter_assignments() 
        except Exception as err:
            print(err)
            raise ServerError()

class CollectorAutomatedAssignmentSync(Resource):
    
    @jwt_required()
    def put(self):

        try:
            
            # get all the automated assignments
            AssignmentModel.sync_automated_assignments()


        except Exception as err:
            raise ServerError()
