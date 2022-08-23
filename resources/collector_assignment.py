from tkinter import E
from flask_restful import Resource, request
from marshmallow import INCLUDE, ValidationError

from models.collector_assignment import AssignmentModel
from validators.assignment import AssignmentIdentitySchema, AssignmentPricesIdentitySchema, AssignmentSchema
from validators.errors import NotFoundError, ServerError, Validation_Error

assignmentSchema = AssignmentSchema()
assignmentIdentitySchema = AssignmentIdentitySchema()
assignmentPricesIdentitySchema = AssignmentPricesIdentitySchema()

class CollectorAssignmentList(Resource):

    def get(self):
        try:
            args = request.args.to_dict()
            return [ assignment.json() for assignment in AssignmentModel.find_all(args) ]
        except Exception as e:
            print(e)
            raise ServerError()

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

    def get(self, id):
        assignment = AssignmentModel.find_by_id(id)
        if assignment:
            return assignment.json()
        return NotFoundError("Assignment")
            

class ActivateAssignments(Resource):
    
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

    def put(self):
        try:
            assignments = request.get_json(silent=True)

            if not isinstance(assignments, list):
                raise ValidationError(["Missing assignments Data!"])

            assignments = [ assignmentPricesIdentitySchema.load(assignment,unknown=INCLUDE) for assignment in assignments ]
            print("Uploading assignments...")
            print(assignments)

            changeAssignments = []

            for item in assignments:
                assignment = AssignmentModel.find_by_id(item["id"])
                #if approved or inactive the assignment cannot be changed
                if assignment and assignment.status not in ["approved", "inactive"]:
                    assignment.update_assignment_price(item["new_price"], item["collected_at"])
                    changeAssignments.append(assignment)
                
            return [assignment.json() for assignment in changeAssignments], 200

        except ValidationError as err:
            print(err)
            raise Validation_Error()
        except Exception as err:
            print(err)
            raise ServerError()

class CollectorAssignmentListByCollector(Resource):
    def get(self, collector_id):
        return [ assignment.json() for assignment in AssignmentModel.find_by_collector(collector_id) ]